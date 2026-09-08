use super::*;
use rcgen::{
    BasicConstraints, CertificateParams, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};

fn millis(year: i32, month: u8, day: u8) -> i64 {
    (rcgen::date_time_ymd(year, month, day).unix_timestamp_nanos() / 1_000_000) as i64
}
fn leaf(names: Vec<String>) -> (CertificateParams, KeyPair) {
    let mut params = CertificateParams::new(names).unwrap();
    params.not_before = rcgen::date_time_ymd(2025, 1, 1);
    params.not_after = rcgen::date_time_ymd(2030, 1, 1);
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    (params, KeyPair::generate().unwrap())
}
fn self_signed(params: CertificateParams, key: KeyPair) -> CertificateBundle {
    CertificateBundle::new(
        vec![params.self_signed(&key).unwrap().der().to_vec()],
        key.serialize_der(),
    )
    .unwrap()
}
#[test]
fn private_self_signed_leaf_still_checks_server_auth_key_validity_and_san() {
    let (params, key) = leaf(vec!["*.example.test".into(), "exact.example.test".into()]);
    let bundle = self_signed(params.clone(), key);
    let validated = validate_certificate(&bundle, millis(2026, 1, 1)).unwrap();
    assert_eq!(
        validated.dns_names,
        vec!["*.example.test", "exact.example.test"]
    );
    validate_hostname(
        bundle.chain_der(),
        &TlsHostname::new("api.example.test").unwrap(),
    )
    .unwrap();
    for host in ["example.test", "nested.api.example.test", "elsewhere.test"] {
        assert!(validate_hostname(bundle.chain_der(), &TlsHostname::new(host).unwrap()).is_err());
    }
    assert!(validate_certificate(&bundle, millis(2024, 1, 1)).is_err());
    assert!(validate_certificate(&bundle, millis(2030, 1, 1)).is_err());
    let mismatch = CertificateBundle::new(
        bundle.chain_der().to_vec(),
        KeyPair::generate().unwrap().serialize_der(),
    )
    .unwrap();
    assert!(validate_certificate(&mismatch, millis(2026, 1, 1)).is_err());
    let mut client_only = params;
    client_only.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    assert!(validate_certificate(
        &self_signed(client_only, KeyPair::generate().unwrap()),
        millis(2026, 1, 1)
    )
    .is_err());
    let (no_san, key) = leaf(vec![]);
    assert!(validate_certificate(&self_signed(no_san, key), millis(2026, 1, 1)).is_err());
    let (too_many, key) = leaf((0..101).map(|n| format!("h{n}.example.test")).collect());
    assert!(validate_certificate(&self_signed(too_many, key), millis(2026, 1, 1)).is_err());
    let mut trailing = bundle.chain_der().to_vec();
    trailing[0].push(0);
    assert!(validate_certificate(
        &CertificateBundle::new(trailing, bundle.private_key_pkcs8_der().to_vec()).unwrap(),
        millis(2026, 1, 1)
    )
    .is_err());
    assert!(validate_certificate(
        &CertificateBundle::new(vec![vec![1, 2, 3]], vec![4, 5, 6]).unwrap(),
        millis(2026, 1, 1)
    )
    .is_err());
}
#[test]
fn full_chain_interval_is_limited_by_shorter_intermediate_and_anchor() {
    let mut root = CertificateParams::new(Vec::<String>::new()).unwrap();
    root.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    root.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
    root.not_before = rcgen::date_time_ymd(2020, 1, 1);
    root.not_after = rcgen::date_time_ymd(2029, 1, 1);
    let root_key = KeyPair::generate().unwrap();
    let root_cert = root.self_signed(&root_key).unwrap();
    let root_issuer = Issuer::new(root.clone(), root_key);
    let mut intermediate = root;
    intermediate.not_before = rcgen::date_time_ymd(2026, 1, 1);
    intermediate.not_after = rcgen::date_time_ymd(2027, 1, 1);
    intermediate
        .distinguished_name
        .push(rcgen::DnType::CommonName, "intermediate");
    let intermediate_key = KeyPair::generate().unwrap();
    let intermediate_cert = intermediate
        .signed_by(&intermediate_key, &root_issuer)
        .unwrap();
    let intermediate_issuer = Issuer::new(intermediate, intermediate_key);
    let (params, key) = leaf(vec!["chain.example.test".into()]);
    let leaf = params.signed_by(&key, &intermediate_issuer).unwrap();
    let chain = vec![
        leaf.der().to_vec(),
        intermediate_cert.der().to_vec(),
        root_cert.der().to_vec(),
    ];
    let bundle = CertificateBundle::new(chain.clone(), key.serialize_der()).unwrap();
    let validated = validate_certificate(&bundle, millis(2026, 6, 1)).unwrap();
    assert_eq!(validated.not_before_unix_millis, millis(2026, 1, 1));
    assert_eq!(validated.not_after_unix_millis, millis(2027, 1, 1));
    assert!(validate_certificate(&bundle, millis(2027, 1, 1)).is_err());
    let mut reordered = chain.clone();
    reordered.swap(1, 2);
    assert!(validate_certificate(
        &CertificateBundle::new(reordered, key.serialize_der()).unwrap(),
        millis(2026, 6, 1)
    )
    .is_err());
    let mut corrupt = chain;
    let length = corrupt[1].len();
    corrupt[1][length - 1] ^= 1;
    assert!(validate_certificate(
        &CertificateBundle::new(corrupt, key.serialize_der()).unwrap(),
        millis(2026, 6, 1)
    )
    .is_err());
}
#[test]
fn sealing_authenticates_identity_version_chain_key_id_nonce_and_ciphertext() {
    let a = SealingKey::new("a", [7; 32]).unwrap();
    assert!(!format!("{a:?}").contains("7, 7"));
    let sealer = CertificateSealer::new("a", vec![a]).unwrap();
    let id = CertificateId::new("resource-a").unwrap();
    let version = CertificateRevision::new(1).unwrap();
    let envelope = sealer
        .seal(&id, version, b"chain", b"secret-private-key")
        .unwrap();
    assert_eq!(
        &**sealer.open(&id, version, b"chain", &envelope).unwrap(),
        b"secret-private-key"
    );
    assert!(sealer
        .open(
            &CertificateId::new("resource-b").unwrap(),
            version,
            b"chain",
            &envelope
        )
        .is_err());
    assert!(sealer
        .open(
            &id,
            CertificateRevision::new(2).unwrap(),
            b"chain",
            &envelope
        )
        .is_err());
    assert!(sealer
        .open(&id, version, b"other-chain", &envelope)
        .is_err());
    let mut changed = envelope.clone();
    changed.ciphertext[0] ^= 1;
    assert!(sealer.open(&id, version, b"chain", &changed).is_err());
    let mut nonce = envelope.clone();
    nonce.nonce[0] ^= 1;
    assert!(sealer.open(&id, version, b"chain", &nonce).is_err());
    let mut format = envelope.clone();
    format.format_version = 2;
    assert!(sealer.open(&id, version, b"chain", &format).is_err());
    let mut key_id = envelope.clone();
    key_id.key_id = "b".into();
    assert!(sealer.open(&id, version, b"chain", &key_id).is_err());
    let aliases = CertificateSealer::new(
        "a",
        vec![
            SealingKey::new("a", [7; 32]).unwrap(),
            SealingKey::new("b", [7; 32]).unwrap(),
        ],
    )
    .unwrap();
    assert!(
        aliases.open(&id, version, b"chain", &key_id).is_err(),
        "same key bytes under another ID must fail authenticated associated data"
    );
    let rotated = CertificateSealer::new(
        "b",
        vec![
            SealingKey::new("a", [7; 32]).unwrap(),
            SealingKey::new("b", [8; 32]).unwrap(),
        ],
    )
    .unwrap();
    let plaintext = rotated.open(&id, version, b"chain", &envelope).unwrap();
    let resealed = rotated.seal(&id, version, b"chain", &plaintext).unwrap();
    assert_eq!(resealed.key_id, "b");
    assert_ne!(resealed.nonce, envelope.nonce);
    let only_new =
        CertificateSealer::new("b", vec![SealingKey::new("b", [8; 32]).unwrap()]).unwrap();
    assert!(only_new.open(&id, version, b"chain", &envelope).is_err());
    assert_eq!(
        &**only_new.open(&id, version, b"chain", &resealed).unwrap(),
        b"secret-private-key"
    );
    let wrong = CertificateSealer::new("a", vec![SealingKey::new("a", [9; 32]).unwrap()]).unwrap();
    assert!(wrong.open(&id, version, b"chain", &envelope).is_err());
    assert!(CertificateSealer::new("a", vec![]).is_err());
    assert!(CertificateSealer::new(
        "a",
        vec![
            SealingKey::new("a", [0; 32]).unwrap(),
            SealingKey::new("a", [0; 32]).unwrap()
        ]
    )
    .is_err());
    assert!(CertificateSealer::new(
        "a",
        (0..9)
            .map(|n| SealingKey::new(format!("key{n}"), [0; 32]).unwrap())
            .collect()
    )
    .is_err());
}
