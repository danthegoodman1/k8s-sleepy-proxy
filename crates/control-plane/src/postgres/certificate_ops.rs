use super::{error::map_postgres_error, PostgresStore};
use crate::{
    certificate::*,
    store::{StoreError, StoreResult},
    Generation,
};
use deadpool_postgres::{GenericClient, Transaction};
use tokio_postgres::Row;

const CERT_COLUMNS: &str = "certificate_id, version, state, not_before_unix_millis, not_after_unix_millis, dns_names, leaf_sha256, chain_der, seal_format, seal_key_id, seal_nonce, sealed_private_key, sealing_revision";
const MAX_OUTBOX: i64 = 100_000;

fn invalid(error: InvalidCertificateResource) -> StoreError {
    StoreError::invalid_argument(error.0)
}
fn rev(value: i64) -> StoreResult<CertificateRevision> {
    CertificateRevision::new(
        u64::try_from(value)
            .map_err(|_| StoreError::internal("invalid durable certificate revision"))?,
    )
    .map_err(invalid)
}
fn next(value: CertificateRevision) -> StoreResult<CertificateRevision> {
    CertificateRevision::new(
        value
            .get()
            .checked_add(1)
            .ok_or_else(|| StoreError::internal("certificate revision exhausted"))?,
    )
    .map_err(invalid)
}
fn check_revision(expected: CertificateRevision, actual: CertificateRevision) -> StoreResult<()> {
    if expected != actual {
        return Err(StoreError::GenerationConflict {
            expected: Generation::new(expected.get()),
            actual: Generation::new(actual.get()),
        });
    }
    Ok(())
}
fn metadata(row: &Row) -> StoreResult<CertificateMetadata> {
    Ok(CertificateMetadata {
        id: CertificateId::new(row.get::<_, String>("certificate_id")).map_err(invalid)?,
        version: rev(row.get("version"))?,
        state: match row.get::<_, &str>("state") {
            "active" => CertificateState::Active,
            "deleted" => CertificateState::Deleted,
            _ => return Err(StoreError::internal("invalid durable certificate state")),
        },
        not_before_unix_millis: row.get("not_before_unix_millis"),
        not_after_unix_millis: row.get("not_after_unix_millis"),
        dns_names: row.get("dns_names"),
        leaf_sha256: row
            .get::<_, Option<Vec<u8>>>("leaf_sha256")
            .unwrap_or_default(),
        sealing_key_id: row.get("seal_key_id"),
        sealing_revision: rev(row.get("sealing_revision"))?,
    })
}
fn chain_digest(chain: &[Vec<u8>]) -> Vec<u8> {
    let mut digest = ring::digest::Context::new(&ring::digest::SHA256);
    for cert in chain {
        digest.update(&(cert.len() as u64).to_be_bytes());
        digest.update(cert);
    }
    digest.finish().as_ref().to_vec()
}
fn bundle(
    sealer: &CertificateSealer,
    row: &Row,
    meta: &CertificateMetadata,
) -> StoreResult<CertificateBundle> {
    let chain: Vec<Vec<u8>> = row
        .get::<_, Option<_>>("chain_der")
        .ok_or_else(|| StoreError::internal("active certificate material is missing"))?;
    let envelope = SealedPrivateKey {
        format_version: row
            .get::<_, Option<i16>>("seal_format")
            .and_then(|n| u8::try_from(n).ok())
            .ok_or_else(|| StoreError::internal("invalid certificate envelope"))?,
        key_id: meta
            .sealing_key_id
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing key ID is missing"))?,
        nonce: row
            .get::<_, Option<Vec<u8>>>("seal_nonce")
            .and_then(|n| n.try_into().ok())
            .ok_or_else(|| StoreError::internal("invalid certificate nonce"))?,
        ciphertext: row
            .get::<_, Option<Vec<u8>>>("sealed_private_key")
            .ok_or_else(|| StoreError::internal("sealed certificate key is missing"))?,
    };
    let key = sealer
        .open(&meta.id, meta.version, &chain_digest(&chain), &envelope)
        .map_err(|e| StoreError::internal(e.0))?;
    CertificateBundle::new(chain, key.to_vec()).map_err(invalid)
}
async fn lock(tx: &Transaction<'_>) -> StoreResult<()> {
    tx.query_one(
        "SELECT revision FROM tls_certificate_revision WHERE singleton FOR UPDATE",
        &[],
    )
    .await
    .map_err(map_postgres_error)?;
    Ok(())
}
async fn now(client: &impl GenericClient) -> StoreResult<i64> {
    Ok(client
        .query_one(
            "SELECT (extract(epoch from clock_timestamp()) * 1000)::bigint",
            &[],
        )
        .await
        .map_err(map_postgres_error)?
        .get(0))
}
async fn certificate(client: &impl GenericClient, id: &CertificateId) -> StoreResult<Option<Row>> {
    client
        .query_opt(
            &format!("SELECT {CERT_COLUMNS} FROM certificates WHERE certificate_id=$1"),
            &[&id.as_str()],
        )
        .await
        .map_err(map_postgres_error)
}
async fn reserve(tx: &Transaction<'_>, count: usize) -> StoreResult<i64> {
    let count = i64::try_from(count)
        .map_err(|_| StoreError::internal("certificate revision reservation overflow"))?;
    // Writes already hold the counter lock. Overflow aborts the entire mutation.
    Ok(tx.query_one("UPDATE tls_certificate_revision SET revision=revision+$1 WHERE singleton RETURNING revision-$1+1", &[&count]).await.map_err(map_postgres_error)?.get(0))
}
async fn event(
    tx: &Transaction<'_>,
    revision: i64,
    host: Option<&str>,
    id: Option<&str>,
    version: Option<i64>,
    kind: &str,
) -> StoreResult<()> {
    tx.execute("INSERT INTO tls_certificate_outbox(revision,hostname,certificate_id,certificate_version,kind) VALUES($1,$2,$3,$4,$5)", &[&revision,&host,&id,&version,&kind]).await.map_err(map_postgres_error)?;
    Ok(())
}
async fn finish(tx: Transaction<'_>) -> StoreResult<()> {
    // Remove only a contiguous prefix. Hard cap also bounds forgotten readers.
    tx.execute("DELETE FROM tls_certificate_outbox WHERE revision <= (SELECT revision-$1 FROM tls_certificate_revision WHERE singleton)", &[&MAX_OUTBOX]).await.map_err(map_postgres_error)?;
    tx.commit().await.map_err(map_postgres_error)
}
async fn invalidate_hosts(
    tx: &Transaction<'_>,
    id: &CertificateId,
    version: CertificateRevision,
    remove: bool,
) -> StoreResult<()> {
    let rows = tx
        .query(
            "SELECT hostname FROM tls_hostname_bindings WHERE certificate_id=$1 ORDER BY hostname",
            &[&id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;
    if rows.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(StoreError::internal(
            "certificate binding inventory exceeds its bound",
        ));
    }
    let start = reserve(tx, rows.len().max(1)).await?;
    let version = version.get() as i64;
    let kind = if remove { "removed" } else { "published" };
    if rows.is_empty() {
        event(tx, start, None, Some(id.as_str()), Some(version), kind).await?;
    } else {
        // A single batch keeps the maximum 1024-host fanout bounded in database
        // round trips. Each event has its own cursor revision, never a shared ID.
        let hosts: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
        tx.execute("UPDATE tls_hostname_bindings b SET revision=$2+u.ordinality-1, certificate_id=CASE WHEN $3 THEN NULL ELSE b.certificate_id END FROM unnest($1::text[]) WITH ORDINALITY u(hostname,ordinality) WHERE b.hostname=u.hostname", &[&hosts,&start,&remove]).await.map_err(map_postgres_error)?;
        tx.execute("INSERT INTO tls_certificate_outbox(revision,hostname,certificate_id,certificate_version,kind) SELECT $2+u.ordinality-1,u.hostname,$3,$4,$5 FROM unnest($1::text[]) WITH ORDINALITY u(hostname,ordinality)", &[&hosts,&start,&id.as_str(),&version,&kind]).await.map_err(map_postgres_error)?;
    }
    Ok(())
}

pub(super) async fn publish(
    store: &PostgresStore,
    request: PublishCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let old = certificate(&tx, &request.id).await?;
    let old_meta = old.as_ref().map(metadata).transpose()?;
    check_revision(
        request.expected_version,
        old_meta
            .as_ref()
            .map_or(CertificateRevision::ZERO, |m| m.version),
    )?;
    if old_meta
        .as_ref()
        .is_some_and(|m| m.state == CertificateState::Deleted)
    {
        return Err(StoreError::AlreadyExists {
            resource: "retired certificate ID",
        });
    }
    let observed_at = now(&tx).await?;
    let hosts = tx
        .query(
            "SELECT hostname FROM tls_hostname_bindings WHERE certificate_id=$1 LIMIT 1025",
            &[&request.id.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;
    if hosts.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(StoreError::internal(
            "certificate binding inventory exceeds its bound",
        ));
    }
    let version = next(request.expected_version)?;
    let sealing_revision = next(
        old_meta
            .as_ref()
            .map_or(CertificateRevision::ZERO, |m| m.sealing_revision),
    )?;
    let keyring = store
        .certificate_sealer
        .clone()
        .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
    let (request, validated, sealed) = work
        .blocking(move || {
            let validated = validate_certificate(&request.bundle, observed_at).map_err(invalid)?;
            for row in hosts {
                validate_hostname(
                    request.bundle.chain_der(),
                    &TlsHostname::new(row.get::<_, String>(0)).map_err(invalid)?,
                )
                .map_err(invalid)?;
            }
            let sealed = keyring
                .seal(
                    &request.id,
                    version,
                    &chain_digest(request.bundle.chain_der()),
                    request.bundle.private_key_pkcs8_der(),
                )
                .map_err(|e| StoreError::internal(e.0))?;
            Ok((request, validated, sealed))
        })
        .await?;
    tx.execute("INSERT INTO certificates(certificate_id,version,state,not_before_unix_millis,not_after_unix_millis,dns_names,leaf_sha256,chain_der,seal_format,seal_key_id,seal_nonce,sealed_private_key,sealing_revision) VALUES($1,$2,'active',$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) ON CONFLICT(certificate_id) DO UPDATE SET version=excluded.version,state=excluded.state,not_before_unix_millis=excluded.not_before_unix_millis,not_after_unix_millis=excluded.not_after_unix_millis,dns_names=excluded.dns_names,leaf_sha256=excluded.leaf_sha256,chain_der=excluded.chain_der,seal_format=excluded.seal_format,seal_key_id=excluded.seal_key_id,seal_nonce=excluded.seal_nonce,sealed_private_key=excluded.sealed_private_key,sealing_revision=excluded.sealing_revision", &[&request.id.as_str(),&(version.get() as i64),&validated.not_before_unix_millis,&validated.not_after_unix_millis,&validated.dns_names,&validated.leaf_sha256,&request.bundle.chain_der(),&(sealed.format_version as i16),&sealed.key_id,&&sealed.nonce[..],&sealed.ciphertext,&(sealing_revision.get() as i64)]).await.map_err(map_postgres_error)?;
    invalidate_hosts(&tx, &request.id, version, false).await?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    finish(tx).await?;
    Ok(result)
}

pub(super) async fn get_metadata(
    store: &PostgresStore,
    id: CertificateId,
) -> StoreResult<Option<CertificateMetadata>> {
    let work = store.certificate_work.acquire()?;
    work.client(store).await?.query_opt("SELECT certificate_id,version,state,not_before_unix_millis,not_after_unix_millis,dns_names,leaf_sha256,seal_key_id,sealing_revision FROM certificates WHERE certificate_id=$1", &[&id.as_str()]).await.map_err(map_postgres_error)?.as_ref().map(metadata).transpose()
}
pub(super) async fn get_binding(
    store: &PostgresStore,
    hostname: TlsHostname,
) -> StoreResult<TlsBinding> {
    let work = store.certificate_work.acquire()?;
    let row = work
        .client(store)
        .await?
        .query_opt(
            "SELECT certificate_id, revision FROM tls_hostname_bindings WHERE hostname=$1",
            &[&hostname.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;
    Ok(TlsBinding {
        hostname,
        certificate_id: row
            .as_ref()
            .and_then(|r| r.get::<_, Option<String>>(0))
            .map(CertificateId::new)
            .transpose()
            .map_err(invalid)?,
        revision: row
            .as_ref()
            .map(|r| rev(r.get(1)))
            .transpose()?
            .unwrap_or(CertificateRevision::ZERO),
    })
}
pub(super) async fn set_binding(
    store: &PostgresStore,
    request: SetTlsBindingRequest,
) -> StoreResult<TlsBinding> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let old = tx
        .query_opt(
            "SELECT revision FROM tls_hostname_bindings WHERE hostname=$1",
            &[&request.hostname.as_str()],
        )
        .await
        .map_err(map_postgres_error)?;
    check_revision(
        request.expected_revision,
        old.map(|r| rev(r.get(0)))
            .transpose()?
            .unwrap_or(CertificateRevision::ZERO),
    )?;
    let mut version = None;
    if let Some(id) = &request.certificate_id {
        let row = certificate(&tx, id).await?.ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
        let meta = metadata(&row)?;
        if meta.state != CertificateState::Active {
            return Err(StoreError::NotFound {
                resource: "active certificate",
            });
        }
        let keyring = store
            .certificate_sealer
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
        let observed_at = now(&tx).await?;
        let hostname = request.hostname.clone();
        let checked_meta = meta.clone();
        work.blocking(move || {
            let material = bundle(&keyring, &row, &checked_meta)?;
            validate_certificate(&material, observed_at).map_err(invalid)?;
            validate_hostname(material.chain_der(), &hostname).map_err(invalid)?;
            Ok(())
        })
        .await?;
        let count:i64 = tx.query_one("SELECT count(*) FROM tls_hostname_bindings WHERE certificate_id=$1 AND hostname<>$2", &[&id.as_str(),&request.hostname.as_str()]).await.map_err(map_postgres_error)?.get(0);
        if count >= MAX_CERTIFICATE_BINDINGS as i64 {
            return Err(StoreError::invalid_argument(
                "certificate exceeds its 1024 binding bound",
            ));
        }
        version = Some(meta.version.get() as i64);
    }
    let revision = reserve(&tx, 1).await?;
    let id = request.certificate_id.as_ref().map(CertificateId::as_str);
    tx.execute("INSERT INTO tls_hostname_bindings(hostname,certificate_id,revision) VALUES($1,$2,$3) ON CONFLICT(hostname) DO UPDATE SET certificate_id=excluded.certificate_id, revision=excluded.revision", &[&request.hostname.as_str(),&id,&revision]).await.map_err(map_postgres_error)?;
    event(
        &tx,
        revision,
        Some(request.hostname.as_str()),
        id,
        version,
        if id.is_some() { "bound" } else { "unbound" },
    )
    .await?;
    finish(tx).await?;
    Ok(TlsBinding {
        hostname: request.hostname,
        certificate_id: request.certificate_id,
        revision: rev(revision)?,
    })
}

pub(super) async fn remove(
    store: &PostgresStore,
    request: RemoveCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let row = certificate(&tx, &request.id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
    let meta = metadata(&row)?;
    check_revision(request.expected_version, meta.version)?;
    if meta.state != CertificateState::Active {
        return Err(StoreError::NotFound {
            resource: "active certificate",
        });
    }
    let version = next(meta.version)?;
    let sealing_revision = next(meta.sealing_revision)?;
    tx.execute("UPDATE certificates SET version=$2,state='deleted',not_before_unix_millis=0,not_after_unix_millis=0,dns_names='{}',leaf_sha256=NULL,chain_der=NULL,seal_format=NULL,seal_key_id=NULL,seal_nonce=NULL,sealed_private_key=NULL,sealing_revision=$3 WHERE certificate_id=$1", &[&request.id.as_str(),&(version.get() as i64),&(sealing_revision.get() as i64)]).await.map_err(map_postgres_error)?;
    invalidate_hosts(&tx, &request.id, version, true).await?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    finish(tx).await?;
    Ok(result)
}

pub(super) async fn resolve(
    store: &PostgresStore,
    request: ResolveTlsCertificateRequest,
) -> StoreResult<TlsCertificateResolution> {
    let work = store.certificate_work.acquire()?;
    // One statement gives a coherent binding + certificate + observed-time view.
    let row=work.client(store).await?.query_one("SELECT b.revision AS view_revision,c.*,(extract(epoch from clock_timestamp())*1000)::bigint AS observed_at FROM (SELECT 1) anchor LEFT JOIN tls_hostname_bindings b ON b.hostname=$1 LEFT JOIN certificates c ON c.certificate_id=b.certificate_id", &[&request.hostname.as_str()]).await.map_err(map_postgres_error)?;
    let view_revision = rev(row.get::<_, Option<i64>>("view_revision").unwrap_or(0))?;
    let observed_at_unix_millis = row.get("observed_at");
    let value = if row.get::<_, Option<String>>("certificate_id").is_none() {
        TlsCertificateValue::Missing
    } else {
        let metadata = metadata(&row)?;
        if metadata.state != CertificateState::Active {
            return Err(StoreError::internal(
                "TLS binding references a removed certificate",
            ));
        }
        let keyring = store
            .certificate_sealer
            .clone()
            .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
        let checked_meta = metadata.clone();
        let hostname = request.hostname.clone();
        let bundle = work
            .blocking(move || {
                let bundle = bundle(&keyring, &row, &checked_meta)?;
                validate_certificate(&bundle, observed_at_unix_millis).map_err(invalid)?;
                validate_hostname(bundle.chain_der(), &hostname).map_err(invalid)?;
                Ok(bundle)
            })
            .await?;
        if request.known_view_revision == Some(view_revision) {
            TlsCertificateValue::Unchanged { metadata }
        } else {
            TlsCertificateValue::Found { metadata, bundle }
        }
    };
    Ok(TlsCertificateResolution {
        hostname: request.hostname,
        view_revision,
        observed_at_unix_millis,
        value,
    })
}

pub(super) async fn reencrypt(
    store: &PostgresStore,
    request: ReencryptCertificateRequest,
) -> StoreResult<CertificateMetadata> {
    let work = store.certificate_work.acquire()?;
    let mut client = work.client(store).await?;
    let tx = client.transaction().await.map_err(map_postgres_error)?;
    lock(&tx).await?;
    let row = certificate(&tx, &request.id)
        .await?
        .ok_or(StoreError::NotFound {
            resource: "certificate",
        })?;
    let meta = metadata(&row)?;
    check_revision(request.expected_version, meta.version)?;
    check_revision(request.expected_sealing_revision, meta.sealing_revision)?;
    if meta.state != CertificateState::Active {
        return Err(StoreError::NotFound {
            resource: "active certificate",
        });
    }
    let keyring = store
        .certificate_sealer
        .clone()
        .ok_or_else(|| StoreError::internal("certificate sealing keys are not configured"))?;
    let checked_meta = meta.clone();
    let sealed = work
        .blocking(move || {
            let material = bundle(&keyring, &row, &checked_meta)?;
            keyring
                .seal(
                    &checked_meta.id,
                    checked_meta.version,
                    &chain_digest(material.chain_der()),
                    material.private_key_pkcs8_der(),
                )
                .map_err(|e| StoreError::internal(e.0))
        })
        .await?;
    let sealing_revision = next(meta.sealing_revision)?;
    tx.execute("UPDATE certificates SET seal_format=$2,seal_key_id=$3,seal_nonce=$4,sealed_private_key=$5,sealing_revision=$6 WHERE certificate_id=$1", &[&request.id.as_str(),&(sealed.format_version as i16),&sealed.key_id,&&sealed.nonce[..],&sealed.ciphertext,&(sealing_revision.get() as i64)]).await.map_err(map_postgres_error)?;
    let result = metadata(&certificate(&tx, &request.id).await?.unwrap())?;
    tx.commit().await.map_err(map_postgres_error)?;
    Ok(result)
}

pub(super) async fn revision(store: &PostgresStore) -> StoreResult<CertificateRevision> {
    let work = store.certificate_work.acquire()?;
    rev(work
        .client(store)
        .await?
        .query_one(
            "SELECT revision FROM tls_certificate_revision WHERE singleton",
            &[],
        )
        .await
        .map_err(map_postgres_error)?
        .get(0))
}
pub(super) async fn changes(
    store: &PostgresStore,
    cursor: CertificateRevision,
    limit: u32,
) -> StoreResult<DurableTlsCertificateChanges> {
    if limit == 0 || limit > MAX_TLS_CHANGE_BATCH {
        return Err(StoreError::invalid_argument(
            "TLS change batch requires 1–1024 events",
        ));
    }
    let work = store.certificate_work.acquire_watch().await?;
    let cursor_i64 = cursor.get() as i64;
    let row=work.client(store).await?.query_one("SELECT c.revision, (SELECT min(revision) FROM tls_certificate_outbox) AS first_revision, COALESCE((SELECT jsonb_agg(to_jsonb(p) ORDER BY revision) FROM (SELECT revision,hostname,certificate_id,certificate_version,kind FROM tls_certificate_outbox WHERE revision>$1 ORDER BY revision LIMIT $2) p),'[]'::jsonb) AS events FROM tls_certificate_revision c WHERE singleton", &[&cursor_i64,&(limit as i64)]).await.map_err(map_postgres_error)?;
    let current: i64 = row.get("revision");
    let first: Option<i64> = row.get("first_revision");
    let reset = cursor_i64 > current || cursor_i64 < first.map_or(current, |n| n - 1);
    let mut events = Vec::new();
    if !reset {
        let values: serde_json::Value = row.get("events");
        for value in values
            .as_array()
            .ok_or_else(|| StoreError::internal("invalid TLS outbox page"))?
        {
            let get = |key: &str| {
                value[key]
                    .as_str()
                    .ok_or_else(|| StoreError::internal("invalid TLS outbox event"))
            };
            events.push(TlsCertificateChange {
                revision: rev(value["revision"]
                    .as_i64()
                    .ok_or_else(|| StoreError::internal("invalid TLS outbox revision"))?)?,
                hostname: value["hostname"]
                    .as_str()
                    .map(TlsHostname::new)
                    .transpose()
                    .map_err(invalid)?,
                certificate_id: value["certificate_id"]
                    .as_str()
                    .map(CertificateId::new)
                    .transpose()
                    .map_err(invalid)?,
                certificate_version: value["certificate_version"].as_i64().map(rev).transpose()?,
                kind: match get("kind")? {
                    "published" => TlsCertificateChangeKind::Published,
                    "bound" => TlsCertificateChangeKind::Bound,
                    "unbound" => TlsCertificateChangeKind::Unbound,
                    "removed" => TlsCertificateChangeKind::Removed,
                    _ => return Err(StoreError::internal("invalid TLS outbox kind")),
                },
            });
        }
    }
    Ok(DurableTlsCertificateChanges {
        cursor: if reset {
            rev(current)?
        } else {
            events.last().map_or(cursor, |e| e.revision)
        },
        reset,
        events,
    })
}

// One statement observes both the commit-ordered global cursor and each host's
// own current view. No chain, sealed key, decryption, or sealer is involved.
pub(super) async fn snapshot(
    store: &PostgresStore,
    hostnames: Vec<TlsHostname>,
) -> StoreResult<TlsBindingSnapshot> {
    if hostnames.len() > MAX_CERTIFICATE_BINDINGS {
        return Err(StoreError::invalid_argument(
            "TLS interests exceed 1024 hosts",
        ));
    }
    let hosts: Vec<_> = hostnames.iter().map(|h| h.as_str()).collect();
    let work = store.certificate_work.acquire_watch().await?;
    let row = work.client(store).await?.query_one(
        "SELECT c.revision, COALESCE((SELECT jsonb_agg(jsonb_build_object('hostname', h.hostname, 'certificate_id', b.certificate_id, 'revision', COALESCE(b.revision,0)) ORDER BY h.ordinality) FROM unnest($1::text[]) WITH ORDINALITY h(hostname,ordinality) LEFT JOIN tls_hostname_bindings b ON b.hostname=h.hostname),'[]'::jsonb) AS bindings FROM tls_certificate_revision c WHERE singleton",
        &[&hosts],
    ).await.map_err(map_postgres_error)?;
    let values: serde_json::Value = row.get("bindings");
    let mut bindings = Vec::with_capacity(hosts.len());
    for v in values
        .as_array()
        .ok_or_else(|| StoreError::internal("invalid TLS snapshot"))?
    {
        bindings.push(TlsBinding {
            hostname: TlsHostname::new(
                v["hostname"]
                    .as_str()
                    .ok_or_else(|| StoreError::internal("invalid TLS snapshot hostname"))?,
            )
            .map_err(invalid)?,
            certificate_id: v["certificate_id"]
                .as_str()
                .map(CertificateId::new)
                .transpose()
                .map_err(invalid)?,
            revision: rev(v["revision"]
                .as_i64()
                .ok_or_else(|| StoreError::internal("invalid TLS snapshot revision"))?)?,
        });
    }
    Ok(TlsBindingSnapshot {
        cursor: rev(row.get("revision"))?,
        bindings,
    })
}
