use std::time::Duration;
use crate::db;
use crate::db::DB;

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String, ex: Option<Duration> },
    Get { key: String },
    Err,
}

fn as_redis_string(value: &str) -> String {
    format!("${}\r\n{value}\r\n", value.len())
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
            _ => Command::Err,
        }
    }

    pub(crate) fn handle(self, db: &DB) -> String {
        match self {
            Command::Ping => {
                "+PONG\r\n".to_owned()
            }
            Command::Echo(value) => {
                as_redis_string(&value)
            }
            Command::Get { key } => {
                match db::get(db, &key) {
                    None => "$-1\r\n".to_owned(),
                    Some(value) => {
                        as_redis_string(&value)
                    }
                }
            }
            Command::Set { key, value, ex } => {
                db::set(db, key, value, ex);
                "+OK\r\n".to_owned()
            }
            Command::Err => "-ERR\r\n".to_owned(),
        }
    }
}
