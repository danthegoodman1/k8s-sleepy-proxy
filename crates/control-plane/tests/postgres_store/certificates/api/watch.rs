use super::*;
use tokio::sync::mpsc;
use tonic::codegen::tokio_stream::wrappers::ReceiverStream;
type Events = tonic::Streaming<pb::WatchTlsCertificatesResponse>;
fn registration(number: u64, hosts: &[(&str, u64)]) -> pb::WatchTlsCertificatesRequest {
    pb::WatchTlsCertificatesRequest {
        registration: number,
        interests: hosts
            .iter()
            .map(|(h, v)| pb::TlsCertificateInterest {
                hostname: (*h).into(),
                known_view_revision: *v,
            })
            .collect(),
    }
}
async fn watch(
    client: &mut ProxyControlPlaneClient<Channel>,
    request: Option<pb::WatchTlsCertificatesRequest>,
) -> TestResult<(mpsc::Sender<pb::WatchTlsCertificatesRequest>, Events)> {
    let (send, recv) = mpsc::channel(1);
    if let Some(request) = request {
        send.send(request).await?;
    }
    let events = client
        .watch_tls_certificates(authorized(ReceiverStream::new(recv), "proxy-secret"))
        .await?
        .into_inner();
    Ok((send, events))
}
async fn next(events: &mut Events) -> TestResult<pb::watch_tls_certificates_response::Value> {
    Ok(
        tokio::time::timeout(Duration::from_secs(4), events.message())
            .await??
            .ok_or("watch closed")?
            .value
            .ok_or("watch value missing")?,
    )
}
async fn changes(events: &mut Events) -> TestResult<pb::TlsCertificateChanges> {
    match next(events).await? {
        pb::watch_tls_certificates_response::Value::Changes(c) => Ok(c),
        other => Err(format!("expected changes, got {other:?}").into()),
    }
}
#[tokio::test]
async fn postgres_tls_watch_two_replicas_registration_shared_rotation_rebind_and_retention(
) -> TestResult {
    database_test(|store, raw, config| async move {
        let second = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let (url1, ca1) = server(store.clone(), tokens(), true, &mut tasks).await?;
            let (url2, ca2) = server(second.clone(), tokens(), true, &mut tasks).await?;
            let mut first = ProxyControlPlaneClient::new(channel(url1, Some(&ca1)).await?);
            let mut other = ProxyControlPlaneClient::new(channel(url2, Some(&ca2)).await?);
            store
                .publish_certificate(publish("A", 0, &["app.example", "shared.example"]))
                .await?;
            let a = store
                .set_tls_binding(bind("app.example", rev(0), Some("A")))
                .await?;
            let shared = store
                .set_tls_binding(bind("shared.example", rev(0), Some("A")))
                .await?;
            let resolved = first
                .resolve_tls_certificate(authorized(query(None), "proxy-secret"))
                .await?
                .into_inner();
            assert_eq!(resolved.view_revision, a.revision.get());
            // Commit through the independent store after resolve, before registration.
            second
                .publish_certificate(publish("A", 1, &["app.example", "shared.example"]))
                .await?;
            let (send1, mut events1) = watch(
                &mut first,
                Some(registration(
                    1,
                    &[
                        ("app.example", a.revision.get()),
                        ("shared.example", shared.revision.get()),
                    ],
                )),
            )
            .await?;
            let (send2, mut events2) = watch(
                &mut other,
                Some(registration(
                    1,
                    &[
                        ("app.example", a.revision.get()),
                        ("shared.example", shared.revision.get()),
                    ],
                )),
            )
            .await?;
            let snapshot1 = match next(&mut events1).await? {
                pb::watch_tls_certificates_response::Value::Snapshot(s) => s,
                _ => panic!(),
            };
            let snapshot2 = match next(&mut events2).await? {
                pb::watch_tls_certificates_response::Value::Snapshot(s) => s,
                _ => panic!(),
            };
            assert_eq!(snapshot1.bindings, snapshot2.bindings);
            assert_eq!(snapshot1.bindings.len(), 2);
            assert!(snapshot1
                .bindings
                .iter()
                .all(|b| b.revision > a.revision.get()));
            let before = store.load_tls_certificate_revision().await?;
            let mut invalid = publish("A", 2, &["app.example", "shared.example"]);
            invalid.bundle =
                CertificateBundle::new(vec![b"bad-chain".to_vec()], b"WATCH-SECRET-MARKER".to_vec())?;
            assert!(second.publish_certificate(invalid).await.is_err());
            assert_eq!(store.load_tls_certificate_revision().await?, before);
            second
                .publish_certificate(publish("A", 2, &["app.example", "shared.example"]))
                .await?;
            let rotation1 = changes(&mut events1).await?;
            let rotation2 = changes(&mut events2).await?;
            assert_eq!(rotation1, rotation2);
            assert_eq!(rotation1.events.len(), 2);
            assert!(rotation1
                .events
                .iter()
                .all(|e| !e.invalidate && e.certificate_id.as_deref() == Some("A")));
            second
                .publish_certificate(publish("B", 0, &["app.example"]))
                .await?;
            let current = second.get_tls_binding(host("app.example")).await?;
            let rebound = second
                .set_tls_binding(bind("app.example", current.revision, Some("B")))
                .await?;
            let binding1 = changes(&mut events1).await?;
            let binding2 = changes(&mut events2).await?;
            assert_eq!(binding1, binding2);
            assert_eq!(binding1.events[0].view_revision, rebound.revision.get());
            assert!(binding1.events[0].invalidate);
            assert_eq!(binding1.events[0].certificate_id.as_deref(), Some("B"));
            let value = first
                .resolve_tls_certificate(authorized(
                    query(Some(current.revision.get())),
                    "proxy-secret",
                ))
                .await?
                .into_inner();
            let Some(pb::resolve_tls_certificate_response::Value::Found(found)) = value.value else {
                panic!()
            };
            assert_eq!(
                found.metadata.unwrap().version,
                1,
                "certificate version is not the view fence"
            );
            second
                .remove_certificate(RemoveCertificateRequest {
                    id: id("A"),
                    expected_version: rev(3),
                })
                .await?;
            let removal1 = changes(&mut events1).await?;
            let removal2 = changes(&mut events2).await?;
            assert_eq!(removal1, removal2);
            assert_eq!(removal1.events.len(), 1);
            assert_eq!(removal1.events[0].hostname, "shared.example");
            assert!(removal1.events[0].invalidate);
            // Controlled retained-history fixture: seed and trim one contiguous
            // unrelated committed prefix in one transaction. Host revisions stay fixed.
            raw.batch_execute("BEGIN; INSERT INTO tls_certificate_outbox(revision,kind) SELECT revision+n,'published' FROM tls_certificate_revision CROSS JOIN generate_series(1,100001) n WHERE singleton; UPDATE tls_certificate_revision SET revision=revision+100001 WHERE singleton; DELETE FROM tls_certificate_outbox WHERE revision<=(SELECT revision-100000 FROM tls_certificate_revision WHERE singleton); COMMIT").await?;
            assert!(matches!(
                next(&mut events1).await?,
                pb::watch_tls_certificates_response::Value::Reset(_)
            ));
            assert!(matches!(
                next(&mut events2).await?,
                pb::watch_tls_certificates_response::Value::Reset(_)
            ));
            drop((send1, send2, events1, events2));
            // Switch replicas and synchronize the old per-host revision under a
            // much newer global cursor without decrypting to discover the view.
            let (_send, mut events) = watch(
                &mut other,
                Some(registration(1, &[("app.example", rebound.revision.get())])),
            )
            .await?;
            let snapshot = match next(&mut events).await? {
                pb::watch_tls_certificates_response::Value::Snapshot(s) => s,
                _ => panic!(),
            };
            assert!(snapshot.cursor > 100_000);
            assert_eq!(snapshot.bindings[0].revision, rebound.revision.get());
            assert!(!format!("{snapshot:?} {rotation1:?} {removal1:?}").contains("WATCH-SECRET-MARKER"));
            assert_eq!(
                raw.query_one("SELECT count(*) FROM instances", &[])
                    .await?
                    .get::<_, i64>(0),
                0
            );
            Ok(())
        }.await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    }).await
}
#[tokio::test]
async fn postgres_tls_watch_native_role_size_initial_timeout_and_capacity() -> TestResult {
    database_test(|store, _raw, mut config| async move {
        config.max_connections = 2;
        let limited = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let (url, ca) = server(limited.clone(), tokens(), true, &mut tasks).await?;
            let connection = channel(url, Some(&ca)).await?;
            let mut client = ProxyControlPlaneClient::new(connection.clone());
            for token in ["operator-secret", "sidecar-secret", "wrong-secret"] {
                let input = tonic::codegen::tokio_stream::iter([registration(1, &[])]);
                let error = client
                    .watch_tls_certificates(authorized(input, token))
                    .await
                    .unwrap_err();
                assert!(matches!(
                    error.code(),
                    Code::PermissionDenied | Code::Unauthenticated
                ));
            }
            assert_eq!(
                client
                    .watch_tls_certificates(tonic::codegen::tokio_stream::iter([registration(
                        1,
                        &[]
                    )]))
                    .await
                    .unwrap_err()
                    .code(),
                Code::Unauthenticated
            );
            let mut web = ProxyControlPlaneClient::new(WebContentType(connection.clone()));
            let mut input = authorized(
                tonic::codegen::tokio_stream::iter([registration(1, &[])]),
                "proxy-secret",
            );
            input
                .metadata_mut()
                .insert("x-forwarded-proto", "https".parse()?);
            assert_eq!(
                web.watch_tls_certificates(input).await.unwrap_err().code(),
                Code::PermissionDenied
            );
            let mut held = Vec::new();
            for _ in 0..16 {
                held.push(watch(&mut client, None).await?);
            }
            let (extra, recv) = mpsc::channel(1);
            assert_eq!(
                client
                    .watch_tls_certificates(authorized(ReceiverStream::new(recv), "proxy-secret"))
                    .await
                    .unwrap_err()
                    .code(),
                Code::ResourceExhausted
            );
            drop(extra);
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                limited.get_certificate_metadata(id("missing"))
            )
            .await??
            .is_none());
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                limited.get_instance(GetInstanceRequest::new(InstanceId::new("ordinary")?))
            )
            .await??
            .is_none());
            for (_, events) in &mut held {
                assert!(
                    tokio::time::timeout(Duration::from_secs(4), events.message())
                        .await??
                        .is_none(),
                    "initial registration must close in3s, not60s"
                );
            }
            drop(held);
            let names: Vec<_> = (0..1024)
                .map(|i| {
                    format!(
                        "{:063}.{}.{}.{}",
                        i,
                        "b".repeat(63),
                        "c".repeat(63),
                        "d".repeat(61)
                    )
                })
                .collect();
            let request = pb::WatchTlsCertificatesRequest {
                registration: 1,
                interests: names
                    .iter()
                    .map(|h| pb::TlsCertificateInterest {
                        hostname: h.clone(),
                        known_view_revision: 0,
                    })
                    .collect(),
            };
            let (send, mut events) = watch(&mut client, Some(request)).await?;
            let snapshot = match next(&mut events).await? {
                pb::watch_tls_certificates_response::Value::Snapshot(s) => s,
                _ => panic!(),
            };
            assert_eq!(snapshot.bindings.len(), 1024);
            assert!(snapshot
                .bindings
                .iter()
                .all(|b| b.revision == 0 && b.certificate_id.is_none()));
            let oversized = pb::WatchTlsCertificatesRequest {
                registration: 2,
                interests: vec![pb::TlsCertificateInterest {
                    hostname: "a".repeat(512 * 1024 + 1),
                    known_view_revision: 0,
                }],
            };
            send.send(oversized).await?;
            assert!(!matches!(
                tokio::time::timeout(Duration::from_secs(4), events.message()).await?,
                Ok(Some(_))
            ));
            drop((send, events));
            for (auth, tls) in [(AuthConfig::NoAuth, true), (tokens(), false)] {
                let (url, ca) = server(store.clone(), auth, tls, &mut tasks).await?;
                let mut insecure =
                    ProxyControlPlaneClient::new(channel(url, tls.then_some(ca.as_str())).await?);
                let mut request = authorized(
                    tonic::codegen::tokio_stream::iter([registration(1, &[])]),
                    "proxy-secret",
                );
                request
                    .metadata_mut()
                    .insert("x-forwarded-proto", "https".parse()?);
                assert_eq!(
                    insecure
                        .watch_tls_certificates(request)
                        .await
                        .unwrap_err()
                        .code(),
                    Code::PermissionDenied
                );
            }
            Ok(())
        }
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    })
    .await
}
#[tokio::test]
async fn postgres_sixteen_registered_watches_poll_with_two_connection_pool_and_ordinary_native_work(
) -> TestResult {
    database_test(|store, _raw, mut config| async move {
        config.max_connections = 2;
        let limited = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let mut tasks = tokio::task::JoinSet::new();
        let result = async {
            let (url, ca) = server(
                RetryingControlPlaneStore::with_default_policy(Arc::new(limited)),
                tokens(),
                true,
                &mut tasks,
            )
            .await?;
            let connection = channel(url, Some(&ca)).await?;
            let mut proxy = ProxyControlPlaneClient::new(connection.clone());
            let mut operator = OperatorControlPlaneClient::new(connection);
            let mut watches = Vec::new();
            for index in 0..16 {
                let (send, mut events) =
                    watch(&mut proxy, Some(registration(1, &[("live.example", 0)]))).await?;
                let pb::watch_tls_certificates_response::Value::Snapshot(snapshot) =
                    next(&mut events)
                        .await
                        .map_err(|error| format!("initial watch[{index}] snapshot: {error}"))?
                else {
                    panic!("every admitted watch must complete a real SQL snapshot");
                };
                assert_eq!(snapshot.bindings.len(), 1);
                assert_eq!(snapshot.bindings[0].revision, 0);
                watches.push((send, events));
            }
            store
                .publish_certificate(publish("live", 0, &["live.example"]))
                .await?;
            let mut binding_revision = rev(0);
            for round in 0..3 {
                // All sixteen streams remain registered while native ordinary
                // lifecycle and route reads use the same two-connection store.
                let binding = store
                    .set_tls_binding(bind(
                        "live.example",
                        binding_revision,
                        (round != 1).then_some("live"),
                    ))
                    .await?;
                binding_revision = binding.revision;
                assert_eq!(
                    operator
                        .get_instance(authorized(
                            pb::GetInstanceRequest {
                                instance_id: "ordinary".into()
                            },
                            "operator-secret",
                        ))
                        .await
                        .unwrap_err()
                        .code(),
                    Code::NotFound
                );
                let (route_send, route_input) = mpsc::channel(1);
                route_send
                    .send(pb::ProxySubscribeRequest {
                        input: Some(pb::proxy_subscribe_request::Input::SubscribeRoute(
                            pb::ProxySubscribeRouteRequest {
                                request_id: format!("ordinary-{round}"),
                                identity: Some(pb::RouteIdentity {
                                    kind: Some(pb::route_identity::Kind::Http(
                                        pb::HttpRouteIdentity {
                                            host: Some(pb::RouteHost {
                                                kind: pb::RouteHostKind::Exact as i32,
                                                host: "ordinary.example".into(),
                                            }),
                                            path_prefix: Some("/".into()),
                                        },
                                    )),
                                }),
                            },
                        )),
                    })
                    .await?;
                let mut route = proxy
                    .subscribe(authorized(ReceiverStream::new(route_input), "proxy-secret"))
                    .await?
                    .into_inner();
                let route_result = tokio::time::timeout(Duration::from_secs(2), route.message())
                    .await??
                    .ok_or("ordinary route stream closed")?;
                assert!(matches!(
                    route_result.output,
                    Some(pb::proxy_subscribe_response::Output::RouteMiss(_))
                ));
                drop((route_send, route));
                for (index, (_, events)) in watches.iter_mut().enumerate() {
                    let event = changes(events).await.map_err(|error| {
                        format!("round[{round}] watch[{index}] changes: {error}")
                    })?;
                    assert_eq!(event.events.len(), 1);
                    assert_eq!(event.events[0].view_revision, binding_revision.get());
                }
            }
            drop(watches);
            // Client cancellation must return all sixteen slots. These are
            // bounded read-only admission retries, with no mutation replay.
            let deadline = tokio::time::Instant::now() + Duration::from_secs(3);
            let mut recovered = Vec::new();
            for _ in 0..16 {
                let pair = tokio::time::timeout_at(deadline, async {
                    loop {
                        match watch(
                            &mut proxy,
                            Some(registration(1, &[("live.example", binding_revision.get())])),
                        )
                        .await
                        {
                            Ok(pair) => break Ok(pair),
                            Err(error)
                                if error
                                    .downcast_ref::<tonic::Status>()
                                    .is_some_and(|s| s.code() == Code::ResourceExhausted) =>
                            {
                                tokio::task::yield_now().await
                            }
                            Err(error) => break Err(error),
                        }
                    }
                })
                .await??;
                recovered.push(pair);
            }
            for (index, (_, events)) in recovered.iter_mut().enumerate() {
                assert!(matches!(
                    next(events)
                        .await
                        .map_err(|error| format!("recovered watch[{index}] snapshot: {error}"))?,
                    pb::watch_tls_certificates_response::Value::Snapshot(_)
                ));
            }
            drop(recovered);
            Ok(())
        }
        .await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    })
    .await
}
#[tokio::test]
async fn postgres_watch_read_slot_survives_cancelled_sql_without_starving_ordinary_work(
) -> TestResult {
    database_test(|_store, raw, mut config| async move {
        config.max_connections = 16;
        let store = PostgresStore::connect(&config)
            .await?
            .with_certificate_sealer(ring("a", &[("a", 7)]));
        let (mut blocker, connection) = tokio_postgres::connect(config.connection_url(), NoTls).await?;
        let mut tasks = tokio::task::JoinSet::new();
        tasks.spawn(async move { connection.await.unwrap() });
        let pid: i32 = blocker
            .query_one("SELECT pg_backend_pid()", &[])
            .await?
            .get(0);
        let tx = blocker.transaction().await?;
        tx.batch_execute("LOCK TABLE tls_hostname_bindings IN ACCESS EXCLUSIVE MODE")
            .await?;
        let mut reads = tokio::task::JoinSet::new();
        let reader = store.clone();
        reads.spawn(async move {
            reader
                .snapshot_tls_bindings(vec![host("held.example")])
                .await
        });
        let result = async {
            tokio::time::timeout(Duration::from_secs(3),async {
                            loop {
                                let count:i64=raw.query_one("SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",&[&pid]).await?.get(0);
                                if count==1 {return Ok::<_,Box<dyn Error+Send+Sync>>(());}
                                tokio::task::yield_now().await;
                            }
                        }).await??;
            reads.abort_all();
            while reads.join_next().await.is_some() {}
            let mut queued = store.load_tls_certificate_changes(rev(0), 1);
            // Drive the actual outbox future while the cancelled bindings SQL
            // remains blocked. Its initial Pending poll alone proves nothing.
            let (waiting, ordinary) = tokio::join!(
                tokio::time::timeout(Duration::from_millis(200), queued.as_mut()),
                tokio::time::timeout(
                    Duration::from_secs(1),
                    store.get_instance(GetInstanceRequest::new(InstanceId::new("ordinary")?)),
                ),
            );
            assert!(waiting.is_err(), "queued read must wait for the active SQL drain");
            assert!(ordinary??.is_none(), "ordinary work progresses while the watch is queued");
            assert_eq!(raw.query_one(
                "SELECT count(*) FROM pg_stat_activity WHERE $1=ANY(pg_blocking_pids(pid)) AND wait_event_type='Lock'",
                &[&pid],
            ).await?.get::<_,i64>(0), 1, "the original SQL is still blocked");
            drop(queued); // Cancel the FIFO waiter independently of the active drain.
            assert!(tokio::time::timeout(
                Duration::from_secs(1),
                store.get_certificate_metadata(id("ordinary-certificate"))
            )
            .await??
            .is_none());
            // The original read remains blocked. Only the guard's bounded
            // discard can release the watch slot for an unrelated outbox read.
            tokio::time::timeout(Duration::from_secs(7), async {
                loop {
                    match store.load_tls_certificate_changes(rev(0), 1).await {
                        Ok(_) => return,
                        Err(StoreError::Unavailable { .. }) => tokio::task::yield_now().await,
                        other => panic!("unexpected watch recovery: {other:?}"),
                    }
                }
            })
            .await?;
            Ok(())
        }.await;
        reads.abort_all();
        while reads.join_next().await.is_some() {}
        tx.rollback().await?;
        drop(blocker);
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    }).await
}
