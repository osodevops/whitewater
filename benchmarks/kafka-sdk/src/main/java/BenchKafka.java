import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

/**
 * Kafka Java SDK benchmark — same BENCH_RESULT contract as
 * scripts/bench-feed.py (Whitewater) and examples/bench_feed.rs
 * (whitewater-rust). Spring Kafka is not needed for the comparison: it
 * delegates to this same kafka-clients producer/consumer underneath.
 *
 * Environment: BOOTSTRAP_SERVERS, BENCH_TOPIC, BENCH_RECORDS,
 * BENCH_BYTES, BENCH_WRITERS (producer threads), BENCH_READ_THREADS
 * (consumers in one group), BENCH_PARTITIONS (records round-robin
 * partition for even spread), BENCH_MODE (all|write|read),
 * BENCH_PRODUCER_* and BENCH_CONSUMER_* overrides.
 */
public final class BenchKafka {
    private BenchKafka() {}

    static int envInt(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) return fallback;
        return Integer.parseInt(value.trim());
    }

    static long envLong(String name, long fallback) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) return fallback;
        return Long.parseLong(value.trim());
    }

    static String env(String name, String fallback) {
        String value = System.getenv(name);
        return (value == null || value.isEmpty()) ? fallback : value.trim();
    }

    static double pct(List<Double> sorted, double q) {
        if (sorted.isEmpty()) return 0.0;
        int index = (int) Math.ceil(sorted.size() * q);
        return sorted.get(Math.min(index, sorted.size()) - 1);
    }

    static void write(String bootstrap, String topic, long records, int bytes,
                      int writers, int partitions) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.serializer", ByteArraySerializer.class.getName());
        props.put("value.serializer", ByteArraySerializer.class.getName());
        // Whitewater parity: acknowledged at ISR quorum, dedup on retry.
        props.put("acks", env("BENCH_PRODUCER_ACKS", "all"));
        props.put("enable.idempotence", env("BENCH_PRODUCER_IDEMPOTENCE", "true"));
        props.put("linger.ms", env("BENCH_PRODUCER_LINGER_MS", "5"));
        props.put("batch.size", env("BENCH_PRODUCER_BATCH_SIZE", "131072"));
        props.put("delivery.timeout.ms", "120000");
        props.put("request.timeout.ms", "30000");

        ConcurrentLinkedQueue<Double> latencies = new ConcurrentLinkedQueue<>();
        AtomicLong acknowledged = new AtomicLong();
        AtomicLong errors = new AtomicLong();
        byte[] payload = new byte[bytes];
        for (int i = 0; i < bytes; i++) payload[i] = (byte) 0xAB;

        long base = records / writers;
        long extra = records % writers;
        List<Thread> threads = new ArrayList<>();
        long wallStart = System.nanoTime();
        for (int w = 0; w < writers; w++) {
            final long share = base + (w < extra ? 1 : 0);
            final long first = w * base + Math.min(w, extra);
            final byte[] recordPayload = payload;
            final Properties producerProps = props;
            Thread thread = new Thread(() -> {
                try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProps)) {
                    for (long i = 0; i < share; i++) {
                        long index = first + i;
                        byte[] key = String.format("key-%012d", index).getBytes();
                        int partition = (int) (index % partitions);
                        long start = System.nanoTime();
                        producer.send(
                            new ProducerRecord<>(topic, partition, null, key, recordPayload),
                            (meta, error) -> {
                                if (error == null) {
                                    latencies.add((System.nanoTime() - start) / 1_000_000.0);
                                    acknowledged.incrementAndGet();
                                } else {
                                    errors.incrementAndGet();
                                }
                            });
                    }
                    producer.flush();
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) thread.join();
        double seconds = (System.nanoTime() - wallStart) / 1_000_000_000.0;
        List<Double> sorted = new ArrayList<>(latencies);
        Collections.sort(sorted);
        long acked = acknowledged.get();
        System.out.printf(
            "BENCH_RESULT {\"system\":\"kafka-java\",\"mode\":\"write\","
            + "\"records\":%d,\"record_bytes\":%d,\"writers\":%d,"
            + "\"partitions\":%d,\"seconds\":%.3f,\"records_per_sec\":%.1f,"
            + "\"mib_per_sec\":%.3f,\"p50_ms\":%.2f,\"p95_ms\":%.2f,"
            + "\"p99_ms\":%.2f,\"errors\":%d}%n",
            acked, bytes, writers, partitions, seconds,
            acked / seconds, acked * (double) bytes / seconds / 1048576.0,
            pct(sorted, 0.50), pct(sorted, 0.95), pct(sorted, 0.99),
            errors.get());
        if (errors.get() > 0) {
            throw new IllegalStateException(errors.get() + " produce errors");
        }
    }

    static void read(String bootstrap, String topic, long records, int readers)
            throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrap);
        props.put("key.deserializer", ByteArrayDeserializer.class.getName());
        props.put("value.deserializer", ByteArrayDeserializer.class.getName());
        props.put("group.id", "bench-sdk-" + System.currentTimeMillis());
        props.put("auto.offset.reset", "earliest");
        // Pure fetch measurement: no offset-commit traffic on the wire.
        props.put("enable.auto.commit", "false");
        // Stock consumer-perf-test fetch shape — Kafka's optimum: small
        // ~1MB fetch responses pipeline under client prefetch, so poll()
        // finds data already staged instead of blocking on a giant
        // response. Whitewater's synchronous pages optimises the
        // opposite direction (fewest round trips at 65,536 records).
        // Measured optimum on this fixture: ~8MB responses balance
        // transfer count against per-response latency (1MB→77k, 8MB→88k,
        // 32MB→57k rec/s on 60k×1KB single partition).
        props.put("fetch.max.bytes", env("BENCH_CONSUMER_FETCH_MAX", "52428800"));
        props.put("max.partition.fetch.bytes",
                  env("BENCH_CONSUMER_PARTITION_FETCH_MAX", "8388608"));
        props.put("fetch.min.bytes", env("BENCH_CONSUMER_FETCH_MIN", "1"));
        props.put("fetch.max.wait.ms", env("BENCH_CONSUMER_FETCH_WAIT_MS", "500"));
        props.put("max.poll.records", env("BENCH_CONSUMER_MAX_POLL", "4096"));

        AtomicLong consumed = new AtomicLong();
        AtomicLong payloadBytes = new AtomicLong();
        CountDownLatch done = new CountDownLatch(readers);
        int partitions = envInt("BENCH_PARTITIONS", 1);
        AtomicLong lastProgress = new AtomicLong(System.nanoTime());
        // Fetch-phase clock: sum of time inside poll() per consumer —
        // kafka-consumer-perf-test's fetch.time.ms methodology, which
        // excludes the group join/rebalance wall time. Whitewater's
        // stateless fetch loop has no join phase, so poll-time is the
        // honest equivalent of its fetch-drain wall clock.
        ConcurrentLinkedQueue<Long> pollNanos = new ConcurrentLinkedQueue<>();
        List<Thread> threads = new ArrayList<>();
        long wallStart = System.nanoTime();
        for (int r = 0; r < readers; r++) {
            final Properties consumerProps = props;
            final int finalReader = r;
            Thread thread = new Thread(() -> {
                try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
                    // Direct partition assignment — no group join, the
                    // fetch-equivalent of Whitewater's stateless
                    // temporary Reader. Rebalance wall time is a group
                    // coordination cost, not read throughput.
                    List<TopicPartition> assigned = new ArrayList<>();
                    for (int p = 0; p < partitions; p++) {
                        if (p % readers == finalReader) {
                            assigned.add(new TopicPartition(topic, p));
                        }
                    }
                    consumer.assign(assigned);
                    consumer.seekToBeginning(assigned);
                    long pollNanosTotal = 0;
                    while (consumed.get() < records) {
                        long pollStart = System.nanoTime();
                        ConsumerRecords<byte[], byte[]> batch =
                            consumer.poll(Duration.ofMillis(500));
                        pollNanosTotal += System.nanoTime() - pollStart;
                        long count = 0;
                        long size = 0;
                        for (ConsumerRecord<byte[], byte[]> record : batch) {
                            count++;
                            size += record.value().length;
                        }
                        if (count > 0) {
                            consumed.addAndGet(count);
                            payloadBytes.addAndGet(size);
                            lastProgress.set(System.nanoTime());
                        }
                        // Stall guard: no records anywhere for 60s means
                        // the group/partition shape wedged.
                        if (System.nanoTime() - lastProgress.get()
                                > 60_000_000_000L) {
                            break;
                        }
                    }
                    pollNanos.add(pollNanosTotal);
                } finally {
                    done.countDown();
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (Thread thread : threads) thread.join();
        double e2e = (System.nanoTime() - wallStart) / 1_000_000_000.0;
        double seconds = pollNanos.stream().mapToLong(Long::longValue).max().orElse(0)
                / 1_000_000_000.0;
        if (seconds <= 0) seconds = e2e;
        long total = consumed.get();
        System.out.printf(
            "BENCH_RESULT {\"system\":\"kafka-java\",\"mode\":\"read\","
            + "\"records\":%d,\"consumers\":%d,\"seconds\":%.3f,"
            + "\"e2e_seconds\":%.3f,\"records_per_sec\":%.1f,"
            + "\"mib_per_sec\":%.3f}%n",
            total, readers, seconds, e2e, total / seconds,
            payloadBytes.get() / seconds / 1048576.0);
    }

    public static void main(String[] args) throws Exception {
        String bootstrap = env("BOOTSTRAP_SERVERS", "localhost:19092");
        String topic = env("BENCH_TOPIC", "bench.events");
        long records = envLong("BENCH_RECORDS", 20_000);
        int bytes = envInt("BENCH_BYTES", 1024);
        int writers = envInt("BENCH_WRITERS", 4);
        int readers = envInt("BENCH_READ_THREADS", Math.max(1, envInt("BENCH_PARTITIONS", 1)));
        int partitions = envInt("BENCH_PARTITIONS", 1);
        String mode = env("BENCH_MODE", "all");
        if (!mode.equals("read")) {
            write(bootstrap, topic, records, bytes, writers, partitions);
        }
        if (!mode.equals("write")) {
            read(bootstrap, topic, records, readers);
        }
    }
}
