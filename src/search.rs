use crate::entries::{Cursor, LogEntry, Logs};
use crate::ToraError;
use futures::future::{err, ok, FutureResult};
use futures::sync::mpsc::Sender;
use reqwest::Client;
use serde::{Serialize, Serializer};
use serde_json::json;
use serde_json::value::{Map, Value};
use std::io;
use tokio::prelude::*;

pub enum CommandMsg {
    More(Vec<LogEntry>),
    Enough,
}

#[derive(Serialize)]
pub struct SearchQuery {
    #[serde(serialize_with = "serialize_query")]
    pub query: String,
    pub size: u32,
    pub sort: Vec<Map<String, Value>>,
    #[serde(rename = "search_after", skip_serializing_if = "Option::is_none")]
    pub cursor: Option<Cursor>,
}

fn serialize_query<S>(query: &str, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    json!({"query_string": {"query": query}}).serialize(s)
}

fn format_entry(entry: &LogEntry) -> String {
    format!(
        "[{:?}] {} -- {} -- {} == {}:\n    {}\n",
        entry.severity, entry.host, entry.app_name, entry.procid, entry.timestamp, entry.msg
    )
}

pub fn print_logs(logs: Vec<LogEntry>) {
    logs.into_iter()
        .for_each(|e| println!("{}", format_entry(&e)))
}

pub struct LogClient {
    pub index: String,
    pub query: SearchQuery,
    pub creds: (String, String),
    pub tx: Sender<CommandMsg>,
    pub logs: Option<Logs>,
    pub cursor: Option<Cursor>,
}

impl LogClient {
    pub fn send(self) -> FutureResult<Self, ToraError> {
        let url = format!(
            "https://tartarus.infra.xerpa.com.br:9998/logstash-xerpa-{}-*/_search",
            &self.index
        );
        let resp = Client::new()
            .get(&url)
            .basic_auth(&self.creds.0, Some(&self.creds.1))
            .json(&self.query)
            .send();
        match resp {
            Err(e) => match e.get_ref().and_then(|e| e.downcast_ref::<io::Error>()) {
                Some(e) if e.kind() == io::ErrorKind::WouldBlock => ok(self),
                _ => err(ToraError),
            },
            Ok(mut resp) => {
                let logs = resp
                    .text()
                    .map_err(|_| ToraError)
                    .and_then(|txt| serde_json::from_str::<Logs>(&txt).map_err(|_| ToraError))
                    .unwrap();
                let cursor = logs.0.last().map(|e| e.cursor.clone());
                let self_ = LogClient {
                    cursor,
                    logs: Some(logs),
                    ..self
                };
                ok(self_)
            }
        }
    }

    pub fn process_logs(mut self) -> FutureResult<Self, ToraError> {
        let logs = self.logs.take();
        if let Some(logs) = logs {
            let tx = self.tx.clone();
            tokio::spawn(
                tx.send(CommandMsg::More(logs.0))
                    .map(|_| ())
                    .map_err(|_| ()),
            );
        };
        let cursor = self.cursor.take();
        let query = SearchQuery {
            cursor,
            ..self.query
        };
        ok(LogClient { query, ..self })
    }
}
