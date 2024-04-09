use std::collections::HashMap;
use std::num::ParseIntError;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Result, Context};

#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    PING,
    Echo(String),
    Set { key: String, value: String },
    Get { key: String },
    Err,
}

type DB = Arc<Mutex<HashMap<String, String>>>;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();
    let db: DB = Arc::new(Mutex::new(HashMap::<String, String>::new()));
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

    let mut buf = [0; 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => {
                println!("Client disconnected: {}", peer_addr);
                return Ok(());
            }
            Ok(_n) => {
                let data: String = String::from_utf8(buf.to_vec()).with_context(
                    || format!("convert buffer from client to string: {buf:?}")
                )?;
                let data = data.trim_end_matches('\0');
                match parse_commands(data) {
                    Ok(commands) => {
                        for command in commands {
                            println!("DEBUG: got command: {command:?}");
                            let response = match command {
                                Command::PING => "+PONG\r\n".to_owned(),
                                Command::Echo(value) => as_redis_string(&value),
                                Command::Get { key } => {
                                    match db.lock().unwrap().get(&key) {
                                        None => "$-1\r\n".to_owned(),
                                        Some(value) => as_redis_string(value),
                                    }
                                }
                                Command::Set { key, value } => {
                                    db.lock().unwrap().insert(key, value);
                                    "+OK\r\n".to_owned()
                                }
                                Command::Err => "-ERR\r\n".to_owned(),
                            };
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
    Ok(tokenz.iter().map(parse_command).collect())
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

fn parse_command(input: &Vec<&str>) -> Command {
    match input.as_slice() {
        ["ping"] | ["PING"] => { Command::PING }
        ["echo", rest @ ..] |
        ["ECHO", rest @ ..] => {
            Command::Echo(rest.join(" "))
        }
        ["set", key, value] |
        ["SET", key, value] => {
            Command::Set {
                key: key.to_string(),
                value: value.to_string(),
            }
        }
        ["get", key] |
        ["GET", key] => {
            Command::Get { key: key.to_string() }
        }
        _ => Command::Err,
    }
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
