extern crate serde;

use clap::{App, Arg};
use futures::future::ok;
use futures::sync::mpsc;
use futures::sync::mpsc::{Receiver, Sender};
use serde_json::value::{Map, Value};
use std::iter::FromIterator;
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::timer::Delay;

use tora::search::{print_logs, CommandMsg, LogClient, SearchQuery};
use tora::ToraError;

fn main() -> Result<(), ToraError> {
    let matches = App::new("tora-rs")
        .about("A poor clone of tora-hs")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("cfg")
                .help("file with credentials and other stuff")
                .takes_value(true)
                .required(false),
        )
        .arg(
            Arg::with_name("index")
                .short("i")
                .long("index")
                .help("index to query")
                .takes_value(true)
                .required(true),
        )
        .arg(
            Arg::with_name("query")
                .short("q")
                .long("query")
                .help("query to search for")
                .takes_value(true)
                .required(true),
        )
        .get_matches();

    let mut mhome = dirs::home_dir();
    let cfg_file = if let Some(ref path) = matches.value_of("config") {
        path
    } else {
        let default_path: &str = match mhome {
            Some(ref mut pb) => {
                pb.push(".torars_rc");
                pb.to_str().unwrap()
            }
            None => panic!("could not find user home"),
        };
        default_path
    };

    let (user, password) = {
        let contents = std::fs::read_to_string(cfg_file).unwrap();
        let parsed: Value = serde_json::from_str(&contents).unwrap();
        let user = parsed["creds"]["user"].clone();
        let user = user.as_str().unwrap().to_string();
        let password = parsed["creds"]["password"].clone();
        let password = password.as_str().unwrap().to_string();
        (user, password)
    };
    let index = matches.value_of("index").unwrap().to_string();
    let query = matches.value_of("query").unwrap().to_string();

    let (tx, rx): (Sender<CommandMsg>, Receiver<CommandMsg>) = mpsc::channel(0);

    let printer = rx
        .for_each(|msg| match msg {
            CommandMsg::More(es) => {
                print_logs(es);
                Ok(())
            }
            CommandMsg::Enough => Err(()),
        })
        .map_err(|_| ());

    let q = SearchQuery {
        cursor: None,
        query: query,
        size: 500,
        sort: vec![
            Map::from_iter(
                vec![("@timestamp".to_string(), Value::String("asc".to_string()))].into_iter(),
            ),
            Map::from_iter(vec![("_id".to_string(), Value::String("asc".to_string()))].into_iter()),
        ],
    };

    let log_client = LogClient {
        index,
        creds: (user.to_string(), password.to_string()),
        query: q,
        tx: tx,
        logs: None,
        cursor: None,
    };
    let poller = future::loop_fn(log_client, move |client| {
        client
            .send()
            .and_then(|client| client.process_logs())
            .and_then(|(client, is_empty)| {
                let delay = if is_empty {
                    Instant::now() + Duration::from_secs(2)
                } else {
                    Instant::now()
                };
                Delay::new(delay).then(|_| ok(client))
            })
            .and_then(|client| Ok(futures::future::Loop::Continue(client)))
    });

    tokio::run(futures::future::lazy(|| {
        tokio::spawn(poller.map_err(|_| ()));
        printer
    }));

    Ok(())
}
