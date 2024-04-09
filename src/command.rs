use std::sync::Arc;
use std::time::Duration;
use crate::{db, parse, Server};
use crate::db::DB;
use crate::parse::{bulk_string, pairs};

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String, ex: Option<Duration> },
    Get { key: String },
    Info,
    Replconf,
    Psync,
    Err,
}


impl Command {
    pub(crate) fn parse(input: &[&str]) -> Command {
        let input_lower: Vec<String> = input.iter().map(|s| s.to_lowercase()).collect();
        let input_lower: Vec<&str> = input_lower.iter().map(|s| s.as_ref()).collect();


        match input_lower.as_slice() {
            // ping
            ["ping"] => Command::Ping,

            // echo value
            ["echo", rest @ ..] => Command::Echo(rest.join(" ")),

            // set key value [px expire]
            ["set", key, value, "px", ex] => {
                let ex_duration = ex.parse::<u64>().map(Duration::from_millis).ok();
                Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ex: ex_duration,
                }
            }
            ["set", key, value] => Command::Set {
                key: key.to_string(),
                value: value.to_string(),
                ex: None,
            },

            // get key
            ["get", key] => Command::Get { key: key.to_string() },

            // info
            ["info", _rest @ ..] => Command::Info,

            ["replconf", _rest @ ..] => Command::Replconf,

            ["psync", _rest @ ..] => Command::Psync,

            _ => Command::Err,
        }
    }

    pub(crate) fn handle(self, db: &DB, server: &Arc<Server>) -> String {
        match &self {
            Command::Ping => {
                "+PONG\r\n".to_owned()
            }
            Command::Echo(value) => {
                bulk_string(Some(value))
            }
            Command::Get { key } => {
                bulk_string(db::get(db, key).as_deref())
            }
            Command::Set { key, value, ex } => {
                db::set(db, key.to_owned(), value.to_string(), ex.to_owned());
                "+OK\r\n".to_owned()
            }
            Command::Info => {
                pairs(server.info().into_iter())
            }
            Command::Replconf => {
                "+OK\r\n".to_owned()
            }
            Command::Psync => {
                format!(
                    "+FULLRESYNC {repl_id} {offset}\r\n",
                    repl_id = server.replid(), offset = server.offset()
                )
            }
            Command::Err => "-ERR\r\n".to_owned(),
        }
    }
}


pub fn parse_commands(data: &str) -> anyhow::Result<Vec<Command>> {
    let tokenz = parse::tokenize(data);
    Ok(tokenz.iter().map(|v| Command::parse(v)).collect())
}
