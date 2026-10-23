//! All headers (or trailers) seen in a single processing phase.

use crate::Error;
use std::collections::HashMap;

/// All headers (or trailers) of a single processing phase.
///
/// Envoy's ext_proc filter sends header values in
/// [`HeaderValue::raw_value`](envoy_types::pb::envoy::config::core::v3::HeaderValue::raw_value)
/// rather than `value`, so both maps here are populated for every header
/// regardless of which proto field was set. A lookup never has to guess which
/// map a header landed in.
///
/// Values are stored one entry per occurrence on the wire: a header sent twice
/// (Set-Cookie, for instance) yields two entries rather than the second
/// overwriting the first. Values are never split on commas; use
/// [`SplitList`](AllHeaders::split_list) for headers that are genuinely comma
/// delimited lists.
///
/// Keys are lower cased, matching what Envoy sends. The accessors are case
/// insensitive; prefer them to indexing the maps directly.
#[derive(Clone, Debug, Default)]
pub struct AllHeaders {
    /// Lower cased header names mapped to their values, one entry per
    /// occurrence, in wire order.
    pub headers: HashMap<String, Vec<String>>,
    /// Lower cased header names mapped to their raw values, one entry per
    /// occurrence, in wire order. Use this for values that are not valid UTF-8.
    pub raw_headers: HashMap<String, Vec<Vec<u8>>>,
}

impl AllHeaders {
    /// Record a header occurrence. Values are appended, not overwritten, so
    /// repeated headers (e.g. `Set-Cookie`) are preserved in wire order.
    fn add(&mut self, name: &str, raw: &[u8]) {
        let name = name.to_lowercase();
        self.headers
            .entry(name.clone())
            .or_default()
            .push(String::from_utf8_lossy(raw).into_owned());
        self.raw_headers.entry(name).or_default().push(raw.to_vec());
    }

    /// Every value sent for `name`, in wire order, or empty.
    pub fn values(&self, name: &str) -> Vec<String> {
        self.headers
            .get(&name.to_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// The first value sent for `name`, if any.
    pub fn get(&self, name: &str) -> Option<String> {
        self.headers
            .get(&name.to_lowercase())
            .and_then(|v| v.first())
            .cloned()
    }

    /// Every value sent for `name` as raw bytes, in wire order, or empty. Use
    /// this for headers whose values are not valid UTF-8.
    pub fn raw_values(&self, name: &str) -> Vec<Vec<u8>> {
        self.raw_headers
            .get(&name.to_lowercase())
            .cloned()
            .unwrap_or_default()
    }

    /// Whether `name` was sent at all, including with an empty value.
    pub fn has(&self, name: &str) -> bool {
        self.headers.contains_key(&name.to_lowercase())
    }

    /// Split the comma separated elements of every value sent for `name`, with
    /// surrounding whitespace trimmed and empty elements dropped.
    ///
    /// This is only correct for headers defined as a comma delimited list
    /// (`Accept`, `Accept-Encoding`, `Cache-Control`, `Vary`, etc.). Do NOT use
    /// it on `Date`, `User-Agent`, `Cookie`, or any header whose value may
    /// contain a comma that is not a separator.
    pub fn split_list(&self, name: &str) -> Vec<String> {
        let mut out = Vec::new();
        for v in self.values(name) {
            for p in v.split(',') {
                let p = p.trim();
                if !p.is_empty() {
                    out.push(p.to_string());
                }
            }
        }
        out
    }
}

/// Generate [`AllHeaders`] from an envoy
/// [`HeaderMap`](envoy_types::pb::envoy::config::core::v3::HeaderMap).
pub(crate) fn gen_headers(
    header_map: &envoy_types::pb::envoy::config::core::v3::HeaderMap,
) -> Result<AllHeaders, Error> {
    let mut headers = AllHeaders::default();
    for h in header_map.headers.iter() {
        if !h.value.is_empty() && !h.raw_value.is_empty() {
            return Err(format!("header {} sets both 'value' and 'raw_value'", h.key).into());
        }
        let raw = if h.raw_value.is_empty() {
            h.value.clone().into_bytes()
        } else {
            h.raw_value.clone()
        };
        headers.add(&h.key, &raw);
    }
    Ok(headers)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::context::build_header_values_from_map;
    use envoy_types::pb::envoy::config::core::v3::HeaderMap;
    use envoy_types::pb::envoy::config::core::v3::HeaderValue as EnvoyHeaderValue;

    fn header_map(pairs: &[(&str, &str)]) -> HeaderMap {
        let headers = pairs
            .iter()
            .map(|(k, v)| EnvoyHeaderValue {
                key: k.to_string(),
                value: String::new(),
                raw_value: v.as_bytes().to_vec(),
            })
            .collect();
        HeaderMap { headers }
    }

    #[test]
    fn test_all_headers_add_and_get() {
        let mut h = AllHeaders::default();
        h.add("X-Foo", b"bar");
        assert!(h.has("x-foo"));
        assert_eq!(h.get("X-FOO").as_deref(), Some("bar"));
        assert_eq!(h.values("x-foo"), vec!["bar".to_string()]);
        assert_eq!(h.raw_values("x-foo"), vec![b"bar".to_vec()]);
        assert_eq!(h.split_list("x-foo"), vec!["bar".to_string()]);
        assert!(!h.has("x-nope"));
    }

    #[test]
    fn test_gen_headers() {
        let mut h = AllHeaders::default();
        h.add("X-Foo", b"bar");
        h.add("x-foo", b"baz");
        assert_eq!(
            h.values("x-foo"),
            vec!["bar".to_string(), "baz".to_string()]
        );

        let hm = header_map(&[("X-Foo", "bar"), (":method", "GET")]);
        let parsed = gen_headers(&hm).unwrap();
        assert_eq!(parsed.get(":method").as_deref(), Some("GET"));
        assert_eq!(parsed.get("x-foo").as_deref(), Some("bar"));

        let bad = HeaderMap {
            headers: vec![EnvoyHeaderValue {
                key: "x-bad".to_string(),
                value: "a".to_string(),
                raw_value: b"b".to_vec(),
            }],
        };
        assert!(gen_headers(&bad).is_err());

        let fallback = HeaderMap {
            headers: vec![EnvoyHeaderValue {
                key: "x-fb".to_string(),
                value: "v".to_string(),
                raw_value: vec![],
            }],
        };
        assert_eq!(
            gen_headers(&fallback).unwrap().get("x-fb").as_deref(),
            Some("v")
        );
    }

    #[test]
    fn test_build_header_values_from_map() {
        let mut m = HashMap::new();
        m.insert("x-foo".to_string(), "bar".to_string());
        let values = build_header_values_from_map(&m);
        assert_eq!(
            values
                .get("x-foo")
                .unwrap()
                .to_envoy_header_value("x-foo")
                .raw_value,
            b"bar".to_vec()
        );
    }
}
