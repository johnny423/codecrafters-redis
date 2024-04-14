use std::time::Duration;

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Replconf {
    ListeningPort(String),
    Capa(String),
    GetAck(String),
    Ack(String)
}

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    Ping,
    Echo(String),
    Set { key: String, value: String, ex: Option<Duration> },
    Get { key: String },
    Info,
    Replconf(Replconf),
    Psync,
    Err,
    Wait,
}


impl Command {
    pub(crate) fn parse(input: &[String]) -> Command {
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

            ["replconf", "listening-port", port] => {
                Command::Replconf(Replconf::ListeningPort(port.to_string()))
            },
            ["replconf", "capa", val] => {
                Command::Replconf(Replconf::Capa(val.to_string()))
            },
            ["replconf", "getack", val] => {
                Command::Replconf(Replconf::GetAck(val.to_string()))
            },
            ["replconf", "ack", val] => {
                Command::Replconf(Replconf::Ack(val.to_string()))
            },

            ["psync", _rest @ ..] => Command::Psync,

            ["wait", _rest @ ..] => Command::Wait,

            _ => Command::Err,
        }
    }
}

