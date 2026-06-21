use std::{error::Error, fmt, io, str};

use tls_parser::{
    parse_tls_client_hello_extensions, parse_tls_handshake_client_hello, SNIType, TlsExtension,
    MAX_RECORD_LEN,
};
use tokio::io::{AsyncRead, AsyncReadExt};

const TLS_RECORD_HEADER_LEN: usize = 5;
const TLS_HANDSHAKE_HEADER_LEN: usize = 4;
const TLS_HANDSHAKE_RECORD_TYPE: u8 = 0x16;
const TLS_CLIENT_HELLO_HANDSHAKE_TYPE: u8 = 0x01;
const TLS_MAJOR_VERSION: u8 = 0x03;
const TLS_VERSION_SSL30: u8 = 0x00;
const TLS_VERSION_TLS13: u8 = 0x04;

pub const MAX_TLS_CLIENT_HELLO_PREFIX_LEN: usize = TLS_RECORD_HEADER_LEN + MAX_RECORD_LEN as usize;

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsClientHelloSni {
    Sni { hostname: String },
    NoSni,
    Incomplete { needed: Option<usize> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TlsClientHelloError {
    NotTls,
    UnsupportedVersion { major: u8, minor: u8 },
    NotClientHello,
    RecordTooLarge { len: usize, max: usize },
    Malformed,
    InvalidHostname,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TlsClientHelloPrefix {
    pub bytes: Vec<u8>,
    pub outcome: Result<TlsClientHelloSni, TlsClientHelloError>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RecordPrefix {
    Complete {
        payload_start: usize,
        payload_end: usize,
    },
    Incomplete {
        needed: usize,
    },
}

pub fn parse_tls_client_hello_sni(prefix: &[u8]) -> Result<TlsClientHelloSni, TlsClientHelloError> {
    let mut cursor = 0;
    let mut handshake = Vec::new();
    let mut expected_handshake_len = None;

    loop {
        let (payload_start, payload_end) = match parse_record_prefix(prefix, cursor)? {
            RecordPrefix::Complete {
                payload_start,
                payload_end,
            } => (payload_start, payload_end),
            RecordPrefix::Incomplete { needed } => {
                return Ok(TlsClientHelloSni::Incomplete {
                    needed: Some(needed),
                })
            }
        };

        let payload = &prefix[payload_start..payload_end];
        if payload.is_empty() {
            return Err(TlsClientHelloError::Malformed);
        }

        handshake.extend_from_slice(payload);

        if handshake[0] != TLS_CLIENT_HELLO_HANDSHAKE_TYPE {
            return Err(TlsClientHelloError::NotClientHello);
        }

        if expected_handshake_len.is_none() && handshake.len() >= TLS_HANDSHAKE_HEADER_LEN {
            let body_len = ((handshake[1] as usize) << 16)
                | ((handshake[2] as usize) << 8)
                | handshake[3] as usize;
            expected_handshake_len = Some(TLS_HANDSHAKE_HEADER_LEN + body_len);
        }

        if let Some(expected_len) = expected_handshake_len {
            if handshake.len() >= expected_len {
                return parse_complete_client_hello(&handshake[..expected_len]);
            }
        }

        cursor = payload_end;
        if cursor >= prefix.len() {
            return Ok(TlsClientHelloSni::Incomplete { needed: Some(1) });
        }
    }
}

pub async fn read_tls_client_hello_prefix<R>(reader: &mut R) -> io::Result<TlsClientHelloPrefix>
where
    R: AsyncRead + Unpin,
{
    read_tls_client_hello_prefix_with_limit(reader, MAX_TLS_CLIENT_HELLO_PREFIX_LEN).await
}

pub async fn read_tls_client_hello_prefix_with_limit<R>(
    reader: &mut R,
    max_prefix_len: usize,
) -> io::Result<TlsClientHelloPrefix>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();

    loop {
        let outcome = parse_tls_client_hello_sni(&bytes);
        let needed = match &outcome {
            Ok(TlsClientHelloSni::Incomplete { needed }) => *needed,
            _ => return Ok(TlsClientHelloPrefix { bytes, outcome }),
        };

        let remaining_capacity = max_prefix_len.saturating_sub(bytes.len());
        if remaining_capacity == 0 {
            return Ok(TlsClientHelloPrefix { bytes, outcome });
        }

        let read_len = needed.unwrap_or(1).max(1).min(remaining_capacity);
        let previous_len = bytes.len();
        bytes.resize(previous_len + read_len, 0);

        let read = reader.read(&mut bytes[previous_len..]).await?;
        bytes.truncate(previous_len + read);

        if read == 0 {
            return Ok(TlsClientHelloPrefix { bytes, outcome });
        }
    }
}

fn parse_record_prefix(prefix: &[u8], offset: usize) -> Result<RecordPrefix, TlsClientHelloError> {
    let record = &prefix[offset..];

    if record.is_empty() {
        return Ok(RecordPrefix::Incomplete { needed: 1 });
    }

    if record[0] != TLS_HANDSHAKE_RECORD_TYPE {
        return if offset == 0 && is_tls_record_type(record[0]) {
            Err(TlsClientHelloError::NotClientHello)
        } else {
            Err(TlsClientHelloError::NotTls)
        };
    }

    if record.len() == 1 {
        return Ok(RecordPrefix::Incomplete { needed: 1 });
    }

    if record[1] != TLS_MAJOR_VERSION {
        return Err(TlsClientHelloError::UnsupportedVersion {
            major: record[1],
            minor: 0,
        });
    }

    if record.len() == 2 {
        return Ok(RecordPrefix::Incomplete { needed: 1 });
    }

    let minor = record[2];
    if !(TLS_VERSION_SSL30..=TLS_VERSION_TLS13).contains(&minor) {
        return Err(TlsClientHelloError::UnsupportedVersion {
            major: record[1],
            minor,
        });
    }

    if record.len() < TLS_RECORD_HEADER_LEN {
        return Ok(RecordPrefix::Incomplete {
            needed: TLS_RECORD_HEADER_LEN - record.len(),
        });
    }

    let len = u16::from_be_bytes([record[3], record[4]]) as usize;
    if len == 0 {
        return Err(TlsClientHelloError::Malformed);
    }
    if len > MAX_RECORD_LEN as usize {
        return Err(TlsClientHelloError::RecordTooLarge {
            len,
            max: MAX_RECORD_LEN as usize,
        });
    }

    let payload_start = offset + TLS_RECORD_HEADER_LEN;
    let payload_end = payload_start + len;
    if prefix.len() < payload_end {
        return Ok(RecordPrefix::Incomplete {
            needed: payload_end - prefix.len(),
        });
    }

    Ok(RecordPrefix::Complete {
        payload_start,
        payload_end,
    })
}

fn parse_complete_client_hello(handshake: &[u8]) -> Result<TlsClientHelloSni, TlsClientHelloError> {
    if handshake.len() < TLS_HANDSHAKE_HEADER_LEN {
        return Err(TlsClientHelloError::Malformed);
    }
    if handshake[0] != TLS_CLIENT_HELLO_HANDSHAKE_TYPE {
        return Err(TlsClientHelloError::NotClientHello);
    }

    let body_len =
        ((handshake[1] as usize) << 16) | ((handshake[2] as usize) << 8) | handshake[3] as usize;
    if handshake.len() != TLS_HANDSHAKE_HEADER_LEN + body_len {
        return Err(TlsClientHelloError::Malformed);
    }

    match parse_tls_handshake_client_hello(&handshake[TLS_HANDSHAKE_HEADER_LEN..]) {
        Ok(([], client_hello)) => extract_sni(client_hello.ext),
        Ok((_, _)) | Err(_) => Err(TlsClientHelloError::Malformed),
    }
}

fn extract_sni(ext: Option<&[u8]>) -> Result<TlsClientHelloSni, TlsClientHelloError> {
    let Some(ext) = ext else {
        return Ok(TlsClientHelloSni::NoSni);
    };

    let (remaining, extensions) =
        parse_tls_client_hello_extensions(ext).map_err(|_| TlsClientHelloError::Malformed)?;
    if !remaining.is_empty() {
        return Err(TlsClientHelloError::Malformed);
    }

    for extension in extensions {
        if let TlsExtension::SNI(names) = extension {
            for (name_type, hostname) in names {
                if name_type == SNIType::HostName {
                    return sni_hostname(hostname);
                }
            }

            return Ok(TlsClientHelloSni::NoSni);
        }
    }

    Ok(TlsClientHelloSni::NoSni)
}

fn sni_hostname(hostname: &[u8]) -> Result<TlsClientHelloSni, TlsClientHelloError> {
    if hostname.is_empty() {
        return Err(TlsClientHelloError::InvalidHostname);
    }

    let hostname = str::from_utf8(hostname).map_err(|_| TlsClientHelloError::InvalidHostname)?;

    Ok(TlsClientHelloSni::Sni {
        hostname: hostname.to_owned(),
    })
}

fn is_tls_record_type(record_type: u8) -> bool {
    matches!(record_type, 0x14..=0x18)
}

impl fmt::Display for TlsClientHelloError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotTls => write!(f, "input does not look like TLS"),
            Self::UnsupportedVersion { major, minor } => {
                write!(
                    f,
                    "unsupported TLS record version {major:#04x}.{minor:#04x}"
                )
            }
            Self::NotClientHello => write!(f, "input is not a TLS ClientHello"),
            Self::RecordTooLarge { len, max } => {
                write!(f, "TLS record length {len} exceeds maximum {max}")
            }
            Self::Malformed => write!(f, "malformed TLS ClientHello"),
            Self::InvalidHostname => write!(f, "invalid TLS SNI hostname"),
        }
    }
}

impl Error for TlsClientHelloError {}

#[cfg(test)]
mod tests {
    use tokio::io::{self, AsyncReadExt, AsyncWriteExt};

    use super::{
        parse_tls_client_hello_sni, read_tls_client_hello_prefix, TlsClientHelloError,
        TlsClientHelloSni,
    };

    #[test]
    fn extracts_sni_without_normalizing_hostname_case() {
        let hello = client_hello(Some(sni_extension(b"MiXeD.Example.COM")));
        let original = hello.clone();

        assert_eq!(
            parse_tls_client_hello_sni(&hello),
            Ok(TlsClientHelloSni::Sni {
                hostname: "MiXeD.Example.COM".to_owned()
            })
        );
        assert_eq!(hello, original);
    }

    #[test]
    fn returns_no_sni_when_client_hello_has_other_extensions() {
        let hello = client_hello(Some(alpn_extension(b"http/1.1")));

        assert_eq!(
            parse_tls_client_hello_sni(&hello),
            Ok(TlsClientHelloSni::NoSni)
        );
    }

    #[test]
    fn returns_incomplete_for_truncated_client_hello_prefix() {
        let hello = client_hello(Some(sni_extension(b"example.com")));

        let needed = hello.len() - 8;
        assert_eq!(
            parse_tls_client_hello_sni(&hello[..8]),
            Ok(TlsClientHelloSni::Incomplete {
                needed: Some(needed)
            })
        );
    }

    #[test]
    fn returns_not_tls_for_non_tls_input() {
        assert_eq!(
            parse_tls_client_hello_sni(b"GET / HTTP/1.1\r\n"),
            Err(TlsClientHelloError::NotTls)
        );
    }

    #[test]
    fn returns_malformed_for_invalid_client_hello_body() {
        let malformed = [0x16, 0x03, 0x03, 0x00, 0x04, 0x01, 0x00, 0x00, 0x00];

        assert_eq!(
            parse_tls_client_hello_sni(&malformed),
            Err(TlsClientHelloError::Malformed)
        );
    }

    #[test]
    fn returns_malformed_for_trailing_junk_inside_client_hello_body() {
        let malformed = client_hello_with_body_trailer(Some(sni_extension(b"example.com")), 0xff);

        assert_eq!(
            parse_tls_client_hello_sni(&malformed),
            Err(TlsClientHelloError::Malformed)
        );
    }

    #[test]
    fn returns_invalid_hostname_for_empty_sni_hostname() {
        let hello = client_hello(Some(sni_extension(b"")));

        assert_eq!(
            parse_tls_client_hello_sni(&hello),
            Err(TlsClientHelloError::InvalidHostname)
        );
    }

    #[test]
    fn parses_client_hello_split_after_handshake_type_byte() {
        let hello = client_hello(Some(sni_extension(b"example.com")));
        let fragmented = fragmented_client_hello(&hello, 1);

        assert_eq!(
            parse_tls_client_hello_sni(&fragmented),
            Ok(TlsClientHelloSni::Sni {
                hostname: "example.com".to_owned()
            })
        );
    }

    #[test]
    fn parses_client_hello_split_inside_sni_extension_block() {
        let hello = client_hello(Some(sni_extension(b"example.com")));
        let handshake_len = hello.len() - 5;
        let split_inside_hostname = handshake_len - 5;
        let fragmented = fragmented_client_hello(&hello, split_inside_hostname);

        assert_eq!(
            parse_tls_client_hello_sni(&fragmented),
            Ok(TlsClientHelloSni::Sni {
                hostname: "example.com".to_owned()
            })
        );
    }

    #[tokio::test]
    async fn async_reader_stops_after_client_hello_prefix() {
        let hello = client_hello(Some(sni_extension(b"example.com")));
        let trailing = b"upstream bytes";
        let mut payload = hello.clone();
        payload.extend_from_slice(trailing);

        let (mut writer, mut reader) = io::duplex(1024);
        let writer_task = tokio::spawn(async move {
            writer.write_all(&payload).await.expect("write payload");
        });

        let prefix = read_tls_client_hello_prefix(&mut reader)
            .await
            .expect("read client hello prefix");

        assert_eq!(prefix.bytes, hello);
        assert_eq!(
            prefix.outcome,
            Ok(TlsClientHelloSni::Sni {
                hostname: "example.com".to_owned()
            })
        );

        let mut remaining = vec![0; trailing.len()];
        reader
            .read_exact(&mut remaining)
            .await
            .expect("read trailing bytes");
        assert_eq!(remaining, trailing);

        writer_task.await.expect("writer task completed");
    }

    #[tokio::test]
    async fn async_reader_stops_after_fragmented_client_hello_prefix() {
        let hello = client_hello(Some(sni_extension(b"example.com")));
        let fragmented = fragmented_client_hello(&hello, 1);
        let trailing = b"upstream bytes";
        let mut payload = fragmented.clone();
        payload.extend_from_slice(trailing);

        let (mut writer, mut reader) = io::duplex(1024);
        let writer_task = tokio::spawn(async move {
            writer.write_all(&payload).await.expect("write payload");
        });

        let prefix = read_tls_client_hello_prefix(&mut reader)
            .await
            .expect("read fragmented client hello prefix");

        assert_eq!(prefix.bytes, fragmented);
        assert_eq!(
            prefix.outcome,
            Ok(TlsClientHelloSni::Sni {
                hostname: "example.com".to_owned()
            })
        );

        let mut remaining = vec![0; trailing.len()];
        reader
            .read_exact(&mut remaining)
            .await
            .expect("read trailing bytes");
        assert_eq!(remaining, trailing);

        writer_task.await.expect("writer task completed");
    }

    fn client_hello(extensions: Option<Vec<u8>>) -> Vec<u8> {
        record(client_hello_handshake(extensions))
    }

    fn client_hello_with_body_trailer(extensions: Option<Vec<u8>>, trailer: u8) -> Vec<u8> {
        let mut body = client_hello_body(extensions);
        body.push(trailer);

        record(handshake(body))
    }

    fn fragmented_client_hello(hello: &[u8], first_record_payload_len: usize) -> Vec<u8> {
        let handshake = &hello[5..];
        assert!(first_record_payload_len > 0);
        assert!(first_record_payload_len < handshake.len());

        let mut fragmented = record(handshake[..first_record_payload_len].to_vec());
        fragmented.extend(record(handshake[first_record_payload_len..].to_vec()));
        fragmented
    }

    fn client_hello_handshake(extensions: Option<Vec<u8>>) -> Vec<u8> {
        handshake(client_hello_body(extensions))
    }

    fn client_hello_body(extensions: Option<Vec<u8>>) -> Vec<u8> {
        let mut body = Vec::new();
        body.extend_from_slice(&[0x03, 0x03]);
        body.extend_from_slice(&[0x11; 32]);
        body.push(0);
        push_u16(&mut body, 2);
        body.extend_from_slice(&[0x13, 0x01]);
        body.push(1);
        body.push(0);

        if let Some(extensions) = extensions {
            push_u16(&mut body, extensions.len());
            body.extend_from_slice(&extensions);
        }

        body
    }

    fn handshake(body: Vec<u8>) -> Vec<u8> {
        let mut handshake = Vec::new();
        handshake.push(0x01);
        push_u24(&mut handshake, body.len());
        handshake.extend_from_slice(&body);
        handshake
    }

    fn record(payload: Vec<u8>) -> Vec<u8> {
        let mut record = Vec::new();
        record.extend_from_slice(&[0x16, 0x03, 0x03]);
        push_u16(&mut record, payload.len());
        record.extend_from_slice(&payload);
        record
    }

    fn sni_extension(hostname: &[u8]) -> Vec<u8> {
        let mut name = Vec::new();
        name.push(0);
        push_u16(&mut name, hostname.len());
        name.extend_from_slice(hostname);

        let mut extension_data = Vec::new();
        push_u16(&mut extension_data, name.len());
        extension_data.extend_from_slice(&name);

        extension(0, extension_data)
    }

    fn alpn_extension(protocol: &[u8]) -> Vec<u8> {
        let mut protocol_name_list = Vec::new();
        protocol_name_list
            .push(u8::try_from(protocol.len()).expect("test protocol length fits in u8"));
        protocol_name_list.extend_from_slice(protocol);

        let mut extension_data = Vec::new();
        push_u16(&mut extension_data, protocol_name_list.len());
        extension_data.extend_from_slice(&protocol_name_list);

        extension(16, extension_data)
    }

    fn extension(extension_type: u16, data: Vec<u8>) -> Vec<u8> {
        let mut extension = Vec::new();
        push_u16(&mut extension, extension_type as usize);
        push_u16(&mut extension, data.len());
        extension.extend_from_slice(&data);
        extension
    }

    fn push_u16(bytes: &mut Vec<u8>, value: usize) {
        let value = u16::try_from(value).expect("test value fits in u16");
        bytes.extend_from_slice(&value.to_be_bytes());
    }

    fn push_u24(bytes: &mut Vec<u8>, value: usize) {
        assert!(value <= 0x00ff_ffff, "test value fits in u24");
        bytes.extend_from_slice(&[
            ((value >> 16) & 0xff) as u8,
            ((value >> 8) & 0xff) as u8,
            (value & 0xff) as u8,
        ]);
    }
}
