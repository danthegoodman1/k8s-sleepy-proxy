use crate::ids::InstanceId;

pub(crate) const DNS_LABEL_MAX_LEN: usize = 63;
const INSTANCE_ID_SUFFIX_RESERVED_LEN: usize = 8;
const INSTANCE_ID_SEPARATOR_LEN: usize = 1;

pub(crate) fn render_instance_scoped_name(base: &str, instance_id: &InstanceId) -> String {
    let suffix = instance_id_suffix(instance_id);
    let base_budget = DNS_LABEL_MAX_LEN - INSTANCE_ID_SEPARATOR_LEN - suffix.len();
    let base = truncate_to_byte_len(base, base_budget);

    format!("{base}-{suffix}")
}

pub(crate) fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= DNS_LABEL_MAX_LEN
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}

fn instance_id_suffix(instance_id: &InstanceId) -> &str {
    let value = instance_id.as_str();
    let suffix_len = value.len().min(INSTANCE_ID_SUFFIX_RESERVED_LEN);
    &value[..suffix_len]
}

fn truncate_to_byte_len(value: &str, max_len: usize) -> &str {
    if value.len() <= max_len {
        return value;
    }

    let mut end = 0;
    for (index, ch) in value.char_indices() {
        let next = index + ch.len_utf8();
        if next > max_len {
            break;
        }
        end = next;
    }

    &value[..end]
}

#[cfg(test)]
mod tests {
    use crate::ids::InstanceId;

    use super::{is_dns_label, render_instance_scoped_name, DNS_LABEL_MAX_LEN};

    #[test]
    fn dns_label_validation_matches_kubernetes_name_budget() {
        assert!(is_dns_label("a"));
        assert!(is_dns_label("tenant-123"));
        assert!(is_dns_label(&"a".repeat(DNS_LABEL_MAX_LEN)));

        for value in ["", "A", "a_b", "a.b", "-a", "a-"] {
            assert!(!is_dns_label(value), "{value:?} should be invalid");
        }
        let overlong = "a".repeat(64);
        assert!(!is_dns_label(&overlong));
    }

    #[test]
    fn instance_scoped_name_preserves_short_instance_id_suffix() {
        assert_eq!(
            render_instance_scoped_name("api", &instance_id("inst")),
            "api-inst"
        );
    }

    #[test]
    fn instance_scoped_name_preserves_eight_character_prefix_for_long_instance_id() {
        assert_eq!(
            render_instance_scoped_name("api", &instance_id("instance-a")),
            "api-instance"
        );
    }

    #[test]
    fn instance_scoped_name_truncates_base_before_suffix() {
        let rendered = render_instance_scoped_name(&"a".repeat(80), &instance_id("instance-a"));

        assert_eq!(rendered.len(), DNS_LABEL_MAX_LEN);
        assert!(rendered.ends_with("-instance"));
        assert!(is_dns_label(&rendered));
    }

    fn instance_id(value: &str) -> InstanceId {
        InstanceId::new(value).expect("valid instance ID")
    }
}
