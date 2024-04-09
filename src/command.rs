use std::sync::Arc;
use std::time::Duration;
use crate::{db, parse, Role, Server};
use crate::db::DB;
use crate::parse::bulk_string;

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String, ex: Option<Duration> },
    Get { key: String },
    Info,
    Err,
}


impl Command {
    pub(crate) fn parse(input: &Vec<&str>) -> Command {
        // todo: better handle case insensitivity
        match input.as_slice() {
            // ping
            ["ping"] | ["PING"] => { Command::Ping }

            // 'echo value'
            ["echo", rest @ ..] |
            ["ECHO", rest @ ..] => {
                // todo should join?? or hold as vec?
                Command::Echo(rest.join(" "))
            }

            // 'set key value [px expire]'
            ["set", key, value, rest @ ..] |
            ["SET", key, value, rest @ ..] => {
                let ex = match rest {
                    ["px", ex] => {
                        Some(Duration::from_millis(ex.parse::<u64>().unwrap()))
                    }
                    _ => None
                };

                Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ex,
                }
            }
            ["get", key] |
            ["GET", key] => {
                Command::Get { key: key.to_string() }
            }
            ["info", _rest @ ..] |
            ["INFO", _rest @ ..] => {
                Command::Info
            }
            _ => Command::Err,
        }
    }

    pub(crate) fn handle(self, db: &DB, server: &Arc<Server>) -> String {
        match self {
            Command::Ping => {
                "+PONG\r\n".to_owned()
            }
            Command::Echo(value) => {
                bulk_string(Some(value))
            }
            Command::Get { key } => {
                bulk_string(db::get(db, &key))
            }
            Command::Set { key, value, ex } => {
                db::set(db, key, value, ex);
                "+OK\r\n".to_owned()
            }
            Command::Info => {
                match server.role {
                    Role::Master => {bulk_string(Some("role:master".to_string()))}
                    Role::Replica { .. } => {bulk_string(Some("role:slave".to_string()))}
                }
            }
            Command::Err => "-ERR\r\n".to_owned(),
        }
    }
}


pub fn parse_commands(data: &str) -> anyhow::Result<Vec<Command>> {
    let tokenz = parse::tokenize(data);
    Ok(tokenz.iter().map(Command::parse).collect())
}
