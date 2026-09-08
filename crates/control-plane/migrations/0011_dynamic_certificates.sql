-- Certificate identity and hostname authority are independent of route/lifecycle
-- tables. The singleton row lock serializes these rare writes through commit.
CREATE TABLE tls_certificate_revision (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    revision bigint NOT NULL CHECK (revision >= 0)
);
INSERT INTO tls_certificate_revision (revision) VALUES (0);
CREATE FUNCTION certificate_chain_octets(bytea[]) RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT sum(octet_length(value)) FROM unnest($1) AS value $$;
CREATE FUNCTION certificate_chain_nonempty(bytea[]) RETURNS boolean
LANGUAGE sql IMMUTABLE STRICT AS $$ SELECT bool_and(value IS NOT NULL AND octet_length(value)>0) FROM unnest($1) AS value $$;
CREATE TABLE certificates (
    certificate_id text PRIMARY KEY,
    version bigint NOT NULL CHECK (version > 0),
    state text NOT NULL CHECK (state IN ('active', 'deleted')),
    not_before_unix_millis bigint NOT NULL,
    not_after_unix_millis bigint NOT NULL,
    dns_names text[] NOT NULL,
    leaf_sha256 bytea,
    chain_der bytea[],
    seal_format smallint,
    seal_key_id text,
    seal_nonce bytea,
    sealed_private_key bytea,
    sealing_revision bigint NOT NULL CHECK (sealing_revision > 0),
    CHECK ((state = 'deleted' AND chain_der IS NULL AND leaf_sha256 IS NULL
        AND seal_format IS NULL AND seal_key_id IS NULL AND seal_nonce IS NULL
        AND sealed_private_key IS NULL AND cardinality(dns_names) = 0)
      OR (state = 'active' AND chain_der IS NOT NULL AND leaf_sha256 IS NOT NULL
        AND seal_format IS NOT NULL AND seal_key_id IS NOT NULL AND seal_nonce IS NOT NULL
        AND sealed_private_key IS NOT NULL AND cardinality(chain_der) BETWEEN 1 AND 16
        AND certificate_chain_nonempty(chain_der)
        AND certificate_chain_octets(chain_der)+octet_length(sealed_private_key)-16 <= 131072
        AND octet_length(leaf_sha256) = 32 AND seal_format = 1
        AND length(seal_key_id) BETWEEN 1 AND 64 AND octet_length(seal_nonce) = 12
        AND octet_length(sealed_private_key) BETWEEN 17 AND 131088
        AND cardinality(dns_names) BETWEEN 1 AND 100
        AND not_before_unix_millis < not_after_unix_millis))
);
CREATE TABLE tls_hostname_bindings (
    hostname text PRIMARY KEY,
    certificate_id text REFERENCES certificates(certificate_id),
    revision bigint NOT NULL CHECK (revision > 0)
);
CREATE INDEX tls_hostname_bindings_certificate ON tls_hostname_bindings(certificate_id) WHERE certificate_id IS NOT NULL;
CREATE TABLE tls_certificate_outbox (
    revision bigint PRIMARY KEY CHECK (revision > 0),
    hostname text,
    certificate_id text,
    certificate_version bigint,
    kind text NOT NULL CHECK (kind IN ('published', 'bound', 'unbound', 'removed')),
    created_at_unix_millis bigint NOT NULL DEFAULT (extract(epoch from clock_timestamp()) * 1000)::bigint
);
