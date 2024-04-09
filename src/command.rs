use std::time::Duration;
use crate::{parse};


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
}


pub fn parse_command(data: &str) -> Command {
    let tokenz = parse::tokenize(data);
    tokenz.first().map(|v| Command::parse(v)).unwrap()
}
