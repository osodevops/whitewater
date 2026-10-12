import pathlib

# ============ tests/pipe_driver.rs ============
p = pathlib.Path('tests/pipe_driver.rs')
t = p.read_text(encoding='utf-8')

old = '''use finnstream::pipe::{
    pipe_declare_request_id, pipe_effect_id, pipe_output_writer_session, FetchedPage,
    FetchedRecord, PipeDriver,
};'''
new = '''use finnstream::pipe::{
    pipe_declare_request_id, pipe_effect_id, pipe_output_writer_session, EnrichResolution,
    FetchedPage, FetchedRecord, PipeDriver,
};'''
assert t.count(old) == 1
t = t.replace(old, new)

tests = '''

fn enrich_pipe() -> PipeDefinition {
    PipeDefinition {
        pipe_id: Uuid::from_u128(0x93),
        name: "orders.enrich".to_owned(),
        space_id: Uuid::from_u128(0xD0),
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        operation: PipeOperation::Enrich {
            key: IndexFieldSpec::Key,
        },
        output_feed_id: Some(Uuid::from_u128(OUTPUT_FEED)),
        output_store_id: Some(Uuid::from_u128(OUTPUT_STORE)),
        stage: PipeStage::Declared,
        status: ResourceStatus::Active,
        created_at_ns: 0,
    }
}

#[tokio::test]
async fn enrich_pipe_stamps_lookup_outcomes_into_output_metadata() {
    let harness = harness_for(enrich_pipe());
    seed_frontier(&harness.progress, "p0").await;

    let mut hit = record(1);
    hit.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"doc-bytes".to_vec(),
    });
    let miss = record(2); // unresolved lookups forward as matched=false
    harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![hit, miss], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let appends = harness.sink.appends.lock().unwrap();
    assert_eq!(appends.len(), 2);
    assert_eq!(appends[0].metadata_base64["enrich.matched"], "dHJ1ZQ==");
    assert_eq!(appends[0].metadata_base64["enrich.payload"], b64(b"doc-bytes"));
    assert_eq!(appends[1].metadata_base64["enrich.matched"], "ZmFsc2U=");
    assert!(!appends[1].metadata_base64.contains_key("enrich.payload"));
    // Input payload and key pass through untouched.
    assert_eq!(appends[0].payload_base64, "cGF5bG9hZA==");
}

#[tokio::test]
async fn enrich_pipe_adopts_a_committed_divergent_declare() {
    let harness = harness_for(enrich_pipe());
    seed_frontier(&harness.progress, "p0").await;
    let effect_id = pipe_effect_id(enrich_pipe().pipe_id, Uuid::from_u128(SUBSCRIPTION), "p0");

    // A raced drive resolved the lookup differently and committed its
    // Declare first: the committed row is the single source of truth.
    let committed_output = EffectOutput {
        feed_id: Uuid::from_u128(OUTPUT_FEED),
        key_base64: "a2V5".to_owned(),
        payload_base64: "cGF5bG9hZA==".to_owned(),
        metadata_base64: BTreeMap::from([
            ("enrich.matched".to_owned(), "dHJ1ZQ==".to_owned()),
            ("enrich.payload".to_owned(), b64(b"stale-but-committed")),
        ]),
        event_time_ns: 1,
        writer_session_id: pipe_output_writer_session(
            enrich_pipe().pipe_id,
            Uuid::from_u128(2),
        ),
        writer_epoch: 1,
        sequence: 1,
    };
    let committed_declare = EffectMutation {
        effect_id,
        subscription_id: Uuid::from_u128(SUBSCRIPTION),
        ownership_epoch: 7,
        sequence: 1,
        request_id: pipe_declare_request_id(effect_id),
        transition: EffectTransition::Declare {
            consume: Some(EffectConsume {
                feed_id: feed(),
                expected_cursor: Some("p0".to_owned()),
                cursor: "p1".to_owned(),
                positions: BTreeMap::from([(range(), "p1".to_owned())]),
            }),
            outputs: vec![committed_output],
            state_mutations: Vec::new(),
        },
    };
    harness.journal.apply(committed_declare).await.unwrap();

    // Our drive plans a divergent resolution — it must adopt the committed
    // Declare rather than conflict or double-decide.
    let mut ours = record(1);
    ours.resolution = Some(EnrichResolution {
        matched: true,
        payload: b"fresher-but-lost".to_vec(),
    });
    let applied = harness
        .driver
        .drive_once(
            64,
            |_f, _l| async { Ok(page(vec![ours], "p1")) },
            || async { Ok(bootstrap_with("p0")) },
            &harness.sink,
        )
        .await
        .unwrap()
        .expect("applied");

    let EffectTransition::Applied { outputs, .. } = &applied.transition else {
        panic!("expected Applied")
    };
    assert_eq!(outputs.len(), 1);
    assert_eq!(
        outputs[0].metadata_base64["enrich.payload"],
        b64(b"stale-but-committed")
    );
    let appends = harness.sink.appends.lock().unwrap();
    assert_eq!(appends.len(), 1);
    assert_eq!(
        appends[0].metadata_base64["enrich.payload"],
        b64(b"stale-but-committed")
    );
}
'''
t = t.rstrip() + tests + '\n'
p.write_text(t, encoding='utf-8')

# ============ pipe.rs unit test ============
p = pathlib.Path('src/pipe.rs')
t = p.read_text(encoding='utf-8')
old = '''    #[test]
    fn forward_pipe_definition_carries_driver_inputs() {'''
new = '''    #[test]
    fn enrich_lookup_key_bytes_follow_the_putrow_byte_contract() {
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Bytes(b"raw".to_vec())),
            Some(b"raw".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Text("txt".to_owned())),
            Some(b"txt".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::I64(42)),
            Some(b"42".to_vec())
        );
        assert_eq!(
            enrich_lookup_key_bytes(&IndexValue::Bool(true)),
            Some(b"true".to_vec())
        );
        assert_eq!(enrich_lookup_key_bytes(&IndexValue::Null), None);
    }

    #[test]
    fn forward_pipe_definition_carries_driver_inputs() {'''
assert t.count(old) == 1
t = t.replace(old, new)
p.write_text(t, encoding='utf-8')

# ============ control.rs enrich validation test additions ============
p = pathlib.Path('src/control.rs')
t = p.read_text(encoding='utf-8')
old = '''        let _ = parsed;
    }
'''
new = '''        let _ = parsed;

        // Enrich Pipes need both targets: an output Feed and a lookup
        // StateStore in the same Domain, plus a valid key projection.
        let enrich = |output_feed, output_store| Command::DefinePipe {
            name: "accounts.enrich".to_owned(),
            subscription: "accounts.flow".to_owned(),
            output_feed,
            output_store,
            operation: PipeOperation::Enrich {
                key: IndexFieldSpec::Key,
            },
        };
        leader
            .execute("CREATE FEED accounts.enriched;")
            .await
            .unwrap();
        for command in [
            enrich(
                Some("accounts.missing".to_owned()),
                Some("accounts.panes".to_owned()),
            ),
            enrich(
                Some("accounts.enriched".to_owned()),
                Some("accounts.missing".to_owned()),
            ),
            enrich(
                Some("accounts.enriched".to_owned()),
                Some("other.panes".to_owned()),
            ),
            // A self-feeding output loop is refused for Enrich too.
            enrich(
                Some("accounts.events".to_owned()),
                Some("accounts.panes".to_owned()),
            ),
            enrich(None, Some("accounts.panes".to_owned())),
            enrich(Some("accounts.enriched".to_owned()), None),
        ] {
            assert!(leader.execute_commands(vec![command]).await.is_err());
        }
        let result = leader
            .execute_commands(vec![enrich(
                Some("accounts.enriched".to_owned()),
                Some("accounts.panes".to_owned()),
            )])
            .await
            .unwrap();
        assert!(result.results[0].data["output_feed_id"].is_string());
        assert!(result.results[0].data["output_store_id"].is_string());
        assert_eq!(result.results[0].data["operation"]["operation"], "enrich");

        // The WCL form: CREATE PIPE ... TO FEED <f> ENRICH STATESTORE <s> BY <spec>.
        leader
            .execute(
                "CREATE PIPE accounts.enriched2 FROM SUBSCRIPTION accounts.flow                  TO FEED accounts.enriched ENRICH STATESTORE accounts.panes BY JSON 'cust';",
            )
            .await
            .unwrap();
        assert!(matches!(
            leader
                .active_pipe_by_name("accounts.enriched2")
                .await
                .unwrap()
                .operation,
            PipeOperation::Enrich {
                key: IndexFieldSpec::Projection { .. }
            }
        ));
        for statement in [
            "CREATE PIPE accounts.bad5 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH STATESTORE accounts.panes;",
            "CREATE PIPE accounts.bad6 FROM SUBSCRIPTION accounts.flow TO FEED accounts.enriched ENRICH accounts.panes BY KEY;",
        ] {
            assert!(
                leader.execute(statement).await.is_err(),
                "{statement} must fail"
            );
        }
    }
'''
assert t.count(old) == 1
t = t.replace(old, new)
p.write_text(t, encoding='utf-8')
print('tests written')
