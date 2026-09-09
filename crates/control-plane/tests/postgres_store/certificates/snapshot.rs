use super::*;

#[tokio::test]
async fn postgres_certificate_conditional_snapshot_watermarks_and_atomic_rollback() -> TestResult {
    database_test(|store, raw, config| async move {
        let names = vec![
            host("a.example.test"),
            host("b.example.test"),
            host("missing.example.test"),
        ];
        let initial = snapshot(&store, names.clone(), None).await?.unwrap();
        assert_eq!(initial.revision, rev(0));
        assert!(initial.bindings.iter().all(|b| b.revision == rev(0)
            && b.last_invalidating_revision == rev(0)
            && b.certificate_id.is_none()));
        assert!(snapshot(&store, names.clone(), Some(initial.revision))
            .await?
            .is_none());
        // A differing future private revision also forces current state; the
        // optimization is equality, never a client authority floor.
        assert_eq!(
            snapshot(&store, names.clone(), Some(rev(99)))
                .await?
                .unwrap()
                .bindings,
            initial.bindings
        );
        store
            .publish_certificate(publish("shared", 0, &["a.example.test", "b.example.test"]))
            .await?;
        let a = store
            .set_tls_binding(bind("a.example.test", rev(0), Some("shared")))
            .await?;
        let b = store
            .set_tls_binding(bind("b.example.test", rev(0), Some("shared")))
            .await?;
        store
            .publish_certificate(publish("shared", 1, &["a.example.test", "b.example.test"]))
            .await?;
        let rotated = snapshot(&store, names.clone(), Some(initial.revision))
            .await?
            .unwrap();
        assert_eq!(rotated.bindings[0].revision, rotated.bindings[1].revision);
        assert_eq!(rotated.bindings[0].last_invalidating_revision, a.revision);
        assert_eq!(rotated.bindings[1].last_invalidating_revision, b.revision);
        assert_eq!(rotated.bindings[2], initial.bindings[2]);
        assert!(snapshot(&store, names.clone(), Some(rotated.revision))
            .await?
            .is_none());
        assert_eq!(
            snapshot(&store, names.clone(), None)
                .await?
                .unwrap()
                .bindings,
            rotated.bindings
        );
        let unbound = store
            .set_tls_binding(bind("a.example.test", rotated.bindings[0].revision, None))
            .await?;
        let rebound = store
            .set_tls_binding(bind("a.example.test", unbound.revision, Some("shared")))
            .await?;
        store
            .publish_certificate(publish("shared", 2, &["a.example.test", "b.example.test"]))
            .await?;
        let coalesced = snapshot(&store, names.clone(), Some(rotated.revision))
            .await?
            .unwrap();
        assert_eq!(coalesced.bindings[0].certificate_id, Some(id("shared")));
        assert_eq!(
            coalesced.bindings[0].last_invalidating_revision,
            rebound.revision
        );
        assert!(coalesced.bindings[0].revision > rebound.revision);
        assert!(coalesced.bindings[0].last_invalidating_revision > rotated.bindings[0].revision);
        let same_id = store
            .set_tls_binding(bind(
                "a.example.test",
                coalesced.bindings[0].revision,
                Some("shared"),
            ))
            .await?;
        assert_eq!(same_id.last_invalidating_revision, same_id.revision);
        assert!(matches!(
            store
                .set_tls_binding(bind("a.example.test", rebound.revision, None))
                .await,
            Err(StoreError::GenerationConflict { .. })
        ));

        // The real singleton lock serializes a writer behind a transaction that
        // rolls back both binding authority and its private global revision.
        let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let mut connections = tokio::task::JoinSet::new();
        connections.spawn(connection);
        let blocker_pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        let before = snapshot(&store, names.clone(), None).await?.unwrap();
        tx.execute(
            "UPDATE tls_certificate_revision SET revision=revision+1 WHERE singleton",
            &[],
        )
        .await?;
        tx.execute("UPDATE tls_hostname_bindings SET certificate_id=NULL,revision=(SELECT revision FROM tls_certificate_revision),last_invalidating_revision=(SELECT revision FROM tls_certificate_revision) WHERE hostname='a.example.test'", &[]).await?;
        assert_eq!(
            snapshot(&store, names.clone(), None)
                .await?
                .unwrap()
                .bindings,
            before.bindings
        );
        assert!(snapshot(&store, names.clone(), Some(before.revision))
            .await?
            .is_none());
        let writer = store.clone();
        let mut mutations = tokio::task::JoinSet::new();
        mutations.spawn(async move {
            writer
                .set_tls_binding(bind("a.example.test", same_id.revision, None))
                .await
        });
        tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    let waiting: i64 = raw.query_one("SELECT count(*) FROM pg_stat_activity WHERE wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid))", &[&blocker_pid]).await.unwrap().get(0);
                    if waiting > 0 { break; }
                    tokio::task::yield_now().await;
                }
            }).await?;
        assert!(mutations.try_join_next().is_none());
        tx.rollback().await?;
        let settled = tokio::time::timeout(Duration::from_secs(3), mutations.join_next())
            .await?
            .unwrap()??;
        assert_eq!(settled.revision.get(), before.revision.get() + 1);
        assert_eq!(settled.last_invalidating_revision, settled.revision);
        connections.abort_all();
        while connections.join_next().await.is_some() {}
        store
            .remove_certificate(RemoveCertificateRequest {
                id: id("shared"),
                expected_version: rev(3),
            })
            .await?;
        let removed = snapshot(&store, names.clone(), None).await?.unwrap();
        assert!(removed.bindings.iter().all(|b| b.certificate_id.is_none()));
        assert_eq!(
            removed.bindings[0].last_invalidating_revision,
            settled.revision
        );
        assert_eq!(removed.bindings[1].revision, removed.revision);
        assert_eq!(
            removed.bindings[1].last_invalidating_revision,
            removed.revision
        );
        assert_eq!(removed.bindings[2], initial.bindings[2]);
        assert!(matches!(
            store
                .snapshot_tls_bindings(vec![host("too-many.example"); 1025], None)
                .await,
            Err(StoreError::InvalidArgument { .. })
        ));

        // Capture the exact production query from its real blocked session,
        // then EXPLAIN that same query; no duplicated SQL implementation here.
        raw.batch_execute("BEGIN; LOCK TABLE tls_hostname_bindings IN ACCESS EXCLUSIVE MODE")
            .await?;
        let raw_pid: i32 = raw.query_one("SELECT pg_backend_pid()", &[]).await?.get(0);
        let reader = store.clone();
        let requested = names.clone();
        let mut reads = tokio::task::JoinSet::new();
        reads.spawn(async move { snapshot(&reader, requested, None).await });
        let sql = tokio::time::timeout(Duration::from_secs(3), async {
                loop {
                    if let Some(row) = raw.query_opt("SELECT query FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND query LIKE '%AS bindings FROM tls_certificate_revision%'", &[&raw_pid]).await.unwrap() {
                        break row.get::<_,String>(0);
                    }
                    tokio::task::yield_now().await;
                }
            }).await?;
        raw.batch_execute("ROLLBACK").await?;
        reads.join_next().await.unwrap()??;
        let hosts: Vec<_> = names.iter().map(TlsHostname::as_str).collect();
        for known in [Some(removed.revision.get() as i64), None] {
            let plan: serde_json::Value = raw
                .query_one(
                    &format!("EXPLAIN (ANALYZE, FORMAT JSON) {sql}"),
                    &[&hosts, &known],
                )
                .await?
                .get(0);
            fn host_loops(value: &serde_json::Value) -> f64 {
                let here = if value["Relation Name"] == "tls_hostname_bindings" {
                    value["Actual Loops"].as_f64().unwrap()
                } else {
                    0.0
                };
                here + value["Plans"]
                    .as_array()
                    .map_or(0.0, |v| v.iter().map(host_loops).sum())
            }
            let loops = host_loops(&plan[0]["Plan"]);
            if known.is_some() {
                assert_eq!(
                    loops, 0.0,
                    "stable global revision must not scan hostname rows"
                );
            } else {
                assert!(loops > 0.0, "forced snapshot exercises the scoped relation");
            }
            println!("conditional_snapshot_plan known={known:?} host_actual_loops={loops} plan={plan}");
        }
        Ok(())
    }).await
}
