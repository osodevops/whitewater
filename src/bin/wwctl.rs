use std::{
    collections::BTreeMap,
    env, fs,
    io::{self, Read, Write},
};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use uuid::Uuid;

use anyhow::{bail, Context, Result};
use finnstream::{
    admin::{AdminClient, CLIENT_API_KEY_ENV},
    api::TemporaryReaderFetchRequest,
    control::ControlExecution,
};

struct CliOptions {
    endpoint: String,
    api_key: Option<String>,
    script: Option<String>,
    write: Option<WriteOptions>,
    read: Option<ReadOptions>,
    json: bool,
}

struct ReadOptions {
    feed: String,
    after: Option<String>,
    limit: Option<usize>,
    wait_ms: Option<u64>,
    new_only: bool,
    tail: bool,
    payload_only: bool,
}

struct WriteOptions {
    writer: String,
    session_epoch: u64,
    request_id: Uuid,
    event_time_ns: Option<i64>,
    key: Vec<u8>,
    payload: Vec<u8>,
    metadata: BTreeMap<String, Vec<u8>>,
}

impl CliOptions {
    fn parse(arguments: impl IntoIterator<Item = String>) -> Result<Option<Self>> {
        let mut endpoint =
            env::var("WHITEWATER_ENDPOINT").unwrap_or_else(|_| "http://127.0.0.1:7071".to_owned());
        let mut api_key = env::var(CLIENT_API_KEY_ENV).ok();
        let mut script = None;
        let mut json = false;
        let mut write_mode = false;
        let mut read_mode = false;
        let mut feed = None;
        let mut after = None;
        let mut limit = None;
        let mut wait_ms = None;
        let mut new_only = false;
        let mut tail = false;
        let mut payload_only = false;
        let mut writer = None;
        let mut session_epoch = None;
        let mut request_id = None;
        let mut event_time_ns = None;
        let mut key = None;
        let mut payload = None;
        let mut metadata = BTreeMap::new();
        let mut arguments = arguments.into_iter();
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "write" => write_mode = true,
                "read" => read_mode = true,
                "--feed" => feed = Some(arguments.next().context("--feed requires a name")?),
                "--after" => after = Some(arguments.next().context("--after requires a Cursor")?),
                "--limit" => {
                    limit = Some(
                        arguments
                            .next()
                            .context("--limit requires a value")?
                            .parse()
                            .context("--limit must be an unsigned integer")?,
                    )
                }
                "--wait" => {
                    wait_ms = Some(
                        arguments
                            .next()
                            .context("--wait requires milliseconds")?
                            .parse()
                            .context("--wait must be an unsigned integer")?,
                    )
                }
                "--new-only" => new_only = true,
                "--tail" => tail = true,
                "--payload-only" => payload_only = true,
                "--writer" => writer = Some(arguments.next().context("--writer requires a name")?),
                "--session-epoch" => {
                    session_epoch = Some(
                        arguments
                            .next()
                            .context("--session-epoch requires a value")?
                            .parse()
                            .context("--session-epoch must be an unsigned integer")?,
                    )
                }
                "--request-id" => {
                    request_id = Some(
                        arguments
                            .next()
                            .context("--request-id requires a UUID")?
                            .parse()
                            .context("--request-id must be a UUID")?,
                    )
                }
                "--event-time-ns" => {
                    event_time_ns = Some(
                        arguments
                            .next()
                            .context("--event-time-ns requires a value")?
                            .parse()
                            .context("--event-time-ns must be a signed integer")?,
                    )
                }
                "--key" => {
                    key = Some(
                        arguments
                            .next()
                            .context("--key requires text")?
                            .into_bytes(),
                    )
                }
                "--key-base64" => {
                    key = Some(
                        STANDARD
                            .decode(arguments.next().context("--key-base64 requires a value")?)
                            .context("invalid --key-base64")?,
                    )
                }
                "--payload" => {
                    payload = Some(
                        arguments
                            .next()
                            .context("--payload requires text")?
                            .into_bytes(),
                    )
                }
                "--payload-base64" => {
                    payload = Some(
                        STANDARD
                            .decode(
                                arguments
                                    .next()
                                    .context("--payload-base64 requires a value")?,
                            )
                            .context("invalid --payload-base64")?,
                    )
                }
                "--payload-file" => {
                    let path = arguments.next().context("--payload-file requires a path")?;
                    payload =
                        Some(fs::read(&path).with_context(|| format!("failed to read {path}"))?);
                }
                "--payload-stdin" => {
                    let mut bytes = Vec::new();
                    io::stdin().read_to_end(&mut bytes)?;
                    payload = Some(bytes);
                }
                "--metadata" => {
                    let value = arguments
                        .next()
                        .context("--metadata requires name=base64")?;
                    let (name, encoded) = value
                        .split_once('=')
                        .context("--metadata requires name=base64")?;
                    metadata.insert(
                        name.to_owned(),
                        STANDARD
                            .decode(encoded)
                            .context("invalid Metadata base64")?,
                    );
                }
                "--endpoint" => {
                    endpoint = arguments.next().context("--endpoint requires a URL")?;
                }
                "--api-key" => {
                    api_key = Some(arguments.next().context("--api-key requires a value")?);
                }
                "-e" | "--execute" => {
                    script = Some(
                        arguments
                            .next()
                            .context("--execute requires a WCL statement")?,
                    );
                }
                "-f" | "--file" => {
                    let path = arguments.next().context("--file requires a path")?;
                    script = Some(
                        fs::read_to_string(&path)
                            .with_context(|| format!("failed to read {path}"))?,
                    );
                }
                "--json" => json = true,
                "-h" | "--help" => return Ok(None),
                value => bail!("unknown argument {value}; use --help"),
            }
        }
        let write = if write_mode {
            Some(WriteOptions {
                writer: writer.context("write requires --writer")?,
                session_epoch: session_epoch.context("write requires --session-epoch")?,
                request_id: request_id.unwrap_or_else(Uuid::new_v4),
                event_time_ns,
                key: key.context("write requires --key or --key-base64")?,
                payload: payload.context("write requires a payload source")?,
                metadata,
            })
        } else {
            None
        };
        if write_mode && read_mode {
            bail!("write and read modes are mutually exclusive");
        }
        if new_only && after.is_some() {
            bail!("--new-only cannot be combined with --after");
        }
        let read = if read_mode {
            Some(ReadOptions {
                feed: feed.context("read requires --feed")?,
                after,
                limit,
                wait_ms,
                new_only,
                tail,
                payload_only,
            })
        } else {
            None
        };
        Ok(Some(Self {
            endpoint,
            api_key,
            script,
            write,
            read,
            json,
        }))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let Some(options) = CliOptions::parse(env::args().skip(1))? else {
        print_help();
        return Ok(());
    };
    let api_key = options.api_key.with_context(|| {
        format!("provide --api-key or set the {CLIENT_API_KEY_ENV} environment variable")
    })?;
    let client = AdminClient::new(options.endpoint, api_key);
    if let Some(write) = options.write {
        let writer = client.writer_session(write.writer, write.session_epoch);
        let result = writer
            .append(
                write.request_id,
                write.event_time_ns,
                &write.key,
                &write.payload,
                &write.metadata,
            )
            .await?;
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else if let Some(read) = options.read {
        let result = client
            .fetch_temporary_reader(&TemporaryReaderFetchRequest {
                feed: read.feed,
                after: read.after,
                limit: read.limit,
                new_only: read.new_only,
                tail: read.tail,
                wait_ms: read.wait_ms,
            })
            .await?;
        if read.payload_only {
            let mut stdout = io::stdout().lock();
            for record in result.records {
                stdout.write_all(&STANDARD.decode(record.payload_base64)?)?;
                stdout.write_all(b"\n")?;
            }
        } else {
            println!("{}", serde_json::to_string_pretty(&result)?);
        }
    } else if let Some(script) = options.script {
        let result = client.execute_wcl(script).await?;
        print_execution(&result, options.json)?;
    } else {
        run_shell(&client, options.json).await?;
    }
    Ok(())
}

async fn run_shell(client: &AdminClient, mut json: bool) -> Result<()> {
    println!("Whitewater Control Language shell");
    println!("All commands use the authenticated Admin API. Type \\help for help.");
    let mut statement = String::new();
    loop {
        print!(
            "{}",
            if statement.is_empty() {
                "whitewater> "
            } else {
                "       ...> "
            }
        );
        io::stdout().flush()?;
        let mut line = String::new();
        if io::stdin().read_line(&mut line)? == 0 {
            println!();
            return Ok(());
        }
        let trimmed = line.trim();
        if statement.is_empty() && trimmed.starts_with('\\') {
            match trimmed {
                "\\q" | "\\quit" | "\\exit" => return Ok(()),
                "\\help" | "\\h" => print_shell_help(),
                "\\json" => {
                    json = !json;
                    println!("JSON output {}", if json { "enabled" } else { "disabled" });
                }
                "\\clear" => statement.clear(),
                "\\g" => println!("No buffered statement."),
                command => println!("Unknown shell command: {command}"),
            }
            continue;
        }
        if trimmed == "\\g" {
            if statement.trim().is_empty() {
                println!("No buffered statement.");
                continue;
            }
        } else {
            statement.push_str(&line);
        }
        if trimmed != "\\g" && !statement_complete(&statement) {
            continue;
        }
        match client.execute_wcl(statement.trim()).await {
            Ok(result) => print_execution(&result, json)?,
            Err(error) => eprintln!("Error: {error}"),
        }
        statement.clear();
    }
}

fn statement_complete(statement: &str) -> bool {
    let mut quoted = false;
    let mut last_unquoted = None;
    let mut chars = statement.chars().peekable();
    while let Some(character) = chars.next() {
        match character {
            '\'' if quoted && chars.peek() == Some(&'\'') => {
                chars.next();
            }
            '\'' => quoted = !quoted,
            character if !quoted && !character.is_whitespace() => last_unquoted = Some(character),
            _ => {}
        }
    }
    !quoted && last_unquoted == Some(';')
}

fn print_execution(execution: &ControlExecution, json: bool) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string_pretty(execution)?);
        return Ok(());
    }
    for result in &execution.results {
        println!("{}: {}", result.statement, result.message);
        if !result.data.is_null() {
            println!("{}", serde_json::to_string_pretty(&result.data)?);
        }
    }
    println!("revision: {} ({})", execution.revision, execution.authority);
    if !execution.warning.is_empty() {
        println!("warning: {}", execution.warning);
    }
    Ok(())
}

fn print_help() {
    println!(
        "wwctl - authenticated Whitewater Admin API shell\n\n\
         Usage:\n  wwctl [--endpoint URL] [--api-key KEY]\n  wwctl [--endpoint URL] [--api-key KEY] --execute \"WCL;\"\n  wwctl [--endpoint URL] [--api-key KEY] --file commands.wcl\n\n\
         Options:\n  --json                Print complete JSON responses\n  -e, --execute WCL     Execute one WCL script and exit\n  -f, --file PATH       Execute a WCL file and exit\n\n\
         Environment:\n  WHITEWATER_ENDPOINT  Defaults to http://127.0.0.1:7071\n  WHITEWATER_API_KEY   Admin API Bearer key"
    );
}

fn print_shell_help() {
    println!(
        "Enter WCL ending with a semicolon. Multi-line statements are supported.\n\
         \\g      Execute the buffered statement without a trailing semicolon\n\
         \\clear  Clear the buffered statement\n\
         \\json   Toggle full JSON output\n\
         \\help   Show this help\n\
         \\quit   Exit the shell"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_complete_statements_without_terminating_inside_quotes() {
        assert!(statement_complete("SHOW FEEDS;"));
        assert!(!statement_complete("SHOW FEEDS"));
        assert!(!statement_complete("SEEK READER audit TO CURSOR 'abc;def'"));
        assert!(statement_complete("SEEK READER audit TO CURSOR 'abc;def';"));
    }

    #[test]
    fn parses_temporary_reader_modes() {
        let options = CliOptions::parse(vec![
            "read".to_owned(),
            "--api-key".to_owned(),
            "development-key-long-enough".to_owned(),
            "--feed".to_owned(),
            "orders.events".to_owned(),
            "--tail".to_owned(),
            "--limit".to_owned(),
            "5".to_owned(),
            "--wait".to_owned(),
            "1000".to_owned(),
            "--payload-only".to_owned(),
        ])
        .unwrap()
        .unwrap();
        let read = options.read.unwrap();
        assert_eq!(read.feed, "orders.events");
        assert_eq!(read.limit, Some(5));
        assert_eq!(read.wait_ms, Some(1000));
        assert!(read.tail);
        assert!(read.payload_only);
        assert!(CliOptions::parse(vec![
            "read".to_owned(),
            "--feed".to_owned(),
            "orders.events".to_owned(),
            "--new-only".to_owned(),
            "--after".to_owned(),
            "cursor".to_owned()
        ])
        .is_err());
    }

    #[test]
    fn parses_writer_session_write_mode() {
        let options = CliOptions::parse(vec![
            "write".to_owned(),
            "--api-key".to_owned(),
            "development-key-long-enough".to_owned(),
            "--writer".to_owned(),
            "checkout".to_owned(),
            "--session-epoch".to_owned(),
            "2".to_owned(),
            "--key".to_owned(),
            "order-1".to_owned(),
            "--payload".to_owned(),
            "created".to_owned(),
            "--metadata".to_owned(),
            "trace-id=dHJhY2U=".to_owned(),
        ])
        .unwrap()
        .unwrap();
        let write = options.write.unwrap();
        assert_eq!(write.writer, "checkout");
        assert_eq!(write.session_epoch, 2);
        assert_eq!(write.key, b"order-1");
        assert_eq!(write.payload, b"created");
        assert_eq!(write.metadata["trace-id"], b"trace");
    }

    #[test]
    fn parses_one_shot_and_interactive_modes() {
        let interactive = CliOptions::parse(Vec::<String>::new()).unwrap().unwrap();
        assert!(interactive.script.is_none());

        let one_shot = CliOptions::parse(vec![
            "--api-key".to_owned(),
            "development-key-long-enough".to_owned(),
            "--json".to_owned(),
            "--execute".to_owned(),
            "SHOW FEEDS;".to_owned(),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(one_shot.script.as_deref(), Some("SHOW FEEDS;"));
        assert!(one_shot.json);
    }
}
