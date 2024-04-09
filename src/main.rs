use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Result, Context};
use crate::Entry::{Expire, Simple};

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    PING,
    Echo(String),
    Set { key: String, value: String, ex: Option<Duration> },
    Get { key: String },
    Err,
}

impl Command {
    fn parse(input: &Vec<&str>) -> Command {
        // todo: better handle case insensitivity
        match input.as_slice() {
            // ping
            ["ping"] | ["PING"] => { Command::PING }

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
                let command = Command::Set {
                    key: key.to_string(),
                    value: value.to_string(),
                    ex,
                };

                command
            }
            ["get", key] |
            ["GET", key] => {
                Command::Get { key: key.to_string() }
            }
            _ => Command::Err,
        }
    }

    fn handle(self, db: &DB) -> String {
        match self {
            Command::PING => {
                "+PONG\r\n".to_owned()
            }
            Command::Echo(value) => {
                as_redis_string(&value)
            }
            Command::Get { key } => {
                match get(&db, &key) {
                    None => "$-1\r\n".to_owned(),
                    Some(value) => {
                        as_redis_string(&value)
                    }
                }
            }
            Command::Set { key, value, ex } => {
                set(db, key, value, ex);
                "+OK\r\n".to_owned()
            }
            Command::Err => "-ERR\r\n".to_owned(),
        }
    }
}


#[derive(Debug)]
enum Entry {
    Simple(String),
    Expire(String, Instant),
}

type DB = Arc<Mutex<HashMap<String, Entry>>>;


fn get(db: &DB, key: &str) -> Option<String> {
    let guard = db.lock().unwrap();
    match guard.get(key) {
        None => { None }
        Some(Entry::Simple(value)) => { Some(value.clone()) }
        Some(Entry::Expire(value, ex)) => {
            if &Instant::now() > ex { None } else {
                Some(value.clone())
            }
        }
    }
}

fn set(db: &DB, key: String, value: String, ex: Option<Duration>) {
    let entry = match ex {
        None => { Simple(value) }
        Some(duration) => { Expire(value, Instant::now() + duration) }
    };
    db.lock().unwrap().insert(key, entry);
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: DB = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));
    println!("Server listening on port 6379");

    while let Ok((stream, _)) = listener.accept().await {
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, db).await {
                eprintln!("Connection failed with: {err}")
            };
        });
    }
}

async fn handle_client(mut stream: TcpStream, db: DB) -> Result<()> {
    let peer_addr = stream.peer_addr().context(
        "fetching peer addr from socket"
    )?;
    println!("Client connected: {}", peer_addr);

    let (mut reader, mut writer) = stream.split();

    // todo better bytes buffer
    let mut buf = [0; 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => {
                println!("Client disconnected: {}", peer_addr);
                return Ok(());
            }
            Ok(_n) => {
                // read client request
                let data: String = String::from_utf8(buf.to_vec()).with_context(
                    || format!("convert buffer from client to string: {buf:?}")
                )?;
                let data = data.trim_end_matches('\0');

                // parse requests into commands
                match parse_commands(data) {
                    Ok(commands) => {
                        for command in commands {
                            println!("DEBUG: got command: {command:?}");
                            // todo: better buffer
                            let response = command.handle(&db);

                            println!("DEBUG: response is {response:?}");
                            writer.write_all(response.as_ref()).await.with_context(
                                || format!("writing response to client {response:?}")
                            )?;
                        }
                    }
                    Err(e) => {
                        eprintln!("ERROR: parsing failed with {e}");
                        writer.write_all(b"-ERR\r\n").await?;
                    }
                }
            }
            Err(err) => {
                eprintln!("Error reading from socket: {}", err);
                return Ok(());
            }
        }
    }
}


fn as_redis_string(value: &str) -> String {
    format!("${}\r\n{value}\r\n", value.len())
}

fn parse_commands(data: &str) -> Result<Vec<Command>> {
    let tokenz = tokenize(data);
    Ok(tokenz.iter().map(Command::parse).collect())
}


fn tokenize(input: &str) -> Vec<Vec<&str>> {
    let mut lines = input.lines();
    let mut result = vec![];
    while let Some(value) = lines.next() {
        if value.is_empty() || !value.starts_with('*') {
            break;
        }

        let length = match value[1..].parse::<usize>() {
            Ok(size) => size,
            Err(_) => break, // todo log error
        };
        let mut command = vec![];
        for i in 0..length {
            if let None = lines.next() {
                eprintln!("ERROR: skipping value length at index {i}, for input: {input:?}");
                break;
            }

            let val = match lines.next() {
                None => {
                    eprintln!("ERROR: parsing value at index {i}, for input: {input:?}");
                    break;
                }
                Some(v) => v,
            };

            command.push(val);
        }
        result.push(command);
    }
    result
}


#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(tokenize(input), vec![vec!["PING"]]);
    }

    #[test]
    fn test_parse_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(parse_commands(input).unwrap(), vec![Command::PING]);
    }

    #[test]
    fn test_full() {
        let input = "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n";
        assert_eq!(tokenize(input), vec![
            vec!["set", "foo", "bar"],
            vec!["get", "foo"],
        ]);
    }
}
