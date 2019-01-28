use chrono::{DateTime, Local};
use serde::{Deserialize, Deserializer};
use serde_json::value::Value;
use std::str::FromStr;

use crate::ToraError;

#[derive(Debug)]
pub struct LogEntry {
    pub msg: String,
    pub timestamp: DateTime<Local>,
    pub severity: Severity,
    pub host: String,
    pub app_name: String,
    pub procid: String,
    pub cursor: Cursor,
}

pub type Cursor = Value;

#[derive(Debug)]
pub struct Logs(pub Vec<LogEntry>);

impl<'de> Deserialize<'de> for Logs {
    fn deserialize<D>(deserializer: D) -> Result<Logs, D::Error>
    where
        D: Deserializer<'de>,
    {
        let mut value: Value = Value::deserialize(deserializer)?;
        let logs: Logs = value
            .as_object_mut()
            .and_then(|o| o.get_mut("hits"))
            .and_then(|v| v.as_object_mut())
            .and_then(|o| o.get_mut("hits"))
            .and_then(|v| v.as_array_mut())
            .and_then(|a| {
                let es = a
                    .iter_mut()
                    .flat_map(|e| parse_entry(e.take()))
                    .collect::<Vec<_>>();
                Some(Logs(es))
            })
            .ok_or(serde::de::Error::custom("bad input"))?;
        Ok(logs)
    }
}

fn parse_timestamp(s: &str) -> Result<chrono::DateTime<chrono::Local>, ToraError> {
    chrono::DateTime::parse_from_rfc3339(s)
        .map_err(|_| ToraError)
        .and_then(|dt| Ok(dt.with_timezone(&chrono::Local)))
}

fn parse_entry(v: Value) -> Result<LogEntry, ToraError> {
    let msg = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("msg").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(|s| Ok(s.to_string()))?;
    let timestamp = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("@timestamp").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(parse_timestamp)?;
    let severity = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("syslog").ok_or(ToraError))
        .and_then(|v| v.get("severity").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(|s| Ok(Severity::from_str(s)))?;
    let app_name = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("syslog").ok_or(ToraError))
        .and_then(|v| v.get("app-name").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(|s| Ok(s.to_string()))?;
    let host = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("syslog").ok_or(ToraError))
        .and_then(|v| v.get("host").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(|s| Ok(s.to_string()))?;
    let procid = v
        .get("_source")
        .ok_or(ToraError)
        .and_then(|v| v.get("syslog").ok_or(ToraError))
        .and_then(|v| v.get("host").ok_or(ToraError))
        .and_then(|v| v.as_str().ok_or(ToraError))
        .and_then(|s| Ok(s.to_string()))?;
    let cursor = v.get("sort").ok_or(ToraError)?.clone();

    Ok(LogEntry {
        msg,
        timestamp,
        severity,
        app_name,
        host,
        procid,
        cursor,
    })
}

#[derive(Debug)]
pub enum Severity {
    Debug,
    Info,
    Warn,
    Notice,
    Error,
    Custom(String),
}

impl Severity {
    fn from_str(s: &str) -> Self {
        use Severity::*;

        match s {
            "debug" => Debug,
            "info" => Info,
            "warning" => Warn,
            "notice" => Notice,
            "err" => Error,
            _ => Custom(String::from_str(s).unwrap_or(String::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::parse_timestamp;
    use super::Logs;
    use crate::search::SearchQuery;
    use chrono::offset::TimeZone;
    use serde_json::json;
    use serde_json::value::{Map, Value};
    use std::iter::FromIterator;

    #[test]
    fn parse0() {
        let txt = include_str!("teste.json");
        assert!(serde_json::from_str::<Logs>(&txt).is_ok());
    }

    #[test]
    fn search_query_no_cursor() {
        let query = SearchQuery {
            cursor: None,
            query: "lukla".to_string(),
            size: 10,
            sort: vec![
                Map::from_iter(
                    vec![("@timestamp".to_string(), Value::String("asc".to_string()))].into_iter(),
                ),
                Map::from_iter(
                    vec![("_id".to_string(), Value::String("asc".to_string()))].into_iter(),
                ),
            ],
        };
        let expected = "{\"query\":{\"query_string\":{\"query\":\"lukla\"}},\"size\":10,\"sort\":[{\"@timestamp\":\"asc\"},{\"_id\":\"asc\"}]}";
        let serialized = serde_json::to_string(&query);
        assert!(serialized.is_ok());
        assert_eq!(serialized.unwrap(), expected);
    }

    #[test]
    fn search_query_with_cursor() {
        let cursor = Value::Array(vec![
            json!(1548460800075u64),
            Value::String("Arx0h2gB5h6KTWImzOwm".to_string()),
        ]);
        let query = SearchQuery {
            cursor: Some(cursor),
            query: "lukla".to_string(),
            size: 10,
            sort: vec![
                Map::from_iter(
                    vec![("@timestamp".to_string(), Value::String("asc".to_string()))].into_iter(),
                ),
                Map::from_iter(
                    vec![("_id".to_string(), Value::String("asc".to_string()))].into_iter(),
                ),
            ],
        };
        let expected = "{\"query\":{\"query_string\":{\"query\":\"lukla\"}},\"size\":10,\"sort\":[{\"@timestamp\":\"asc\"},{\"_id\":\"asc\"}],\"search_after\":[1548460800075,\"Arx0h2gB5h6KTWImzOwm\"]}";
        let serialized = serde_json::to_string(&query);
        assert!(serialized.is_ok());
        assert_eq!(serialized.unwrap(), expected);
    }

    #[test]
    fn test_parse_timestamp() {
        let raw = "2019-01-26T00:00:00.074Z";
        let parsed = parse_timestamp(raw);
        assert!(parsed.is_ok());
        let dt = parsed.unwrap();
        let expected = chrono::Local.ymd(2019, 1, 25).and_hms_milli(22, 0, 0, 74);
        assert_eq!(dt, expected);
    }
}
