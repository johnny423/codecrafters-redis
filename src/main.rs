use std::io::{BufReader, Read, Write};
use std::net::{TcpListener};
use anyhow::{Context, Result};
use crate::Command::PING;


#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
enum Command {
    PING,
    // Set { key: String, value: String },
    // Get { key: String },
}

fn main() -> Result<()> {
    // You can use print statements as follows for debugging, they'll be visible when running tests.
    let address = "127.0.0.1:6379";
    let listener = TcpListener::bind(address).with_context(
        || format!("tcp bind to {address}")
    )?;
    println!("listening to {address}");


    for stream in listener.incoming() {
        match stream {
            Ok(mut stream) => {
                println!("got new stream");
                let mut buf = [0; 512];
                stream.read(&mut buf).unwrap();
                stream.write(b"+PONG\r\n").unwrap();
                // let mut reader = BufReader::new(&stream);
                // let mut data = String::new();
                // reader.read_to_string(&mut data).with_context(
                //     || "failed reading to string"
                // )?;
                // println!("got {data}");
                // let commands: Vec<Command> = parse_commands(&data);
                // for command in commands {
                //     match command {
                //         PING => { stream.write_all(b"+PONG\r\n")?; }
                //     }
                // }
                // stream.write_all(b"+");
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
    Ok(())
}

fn parse_commands(data: &str) -> Vec<Command> {
    tokenize(&data).iter()
        .filter_map(
            |t| parse_command(t)
        )
        .collect()
}


fn tokenize(input: &str) -> Vec<Vec<&str>> {
    let mut lines = input.lines();
    let mut result = vec![];
    while let Some(value) = lines.next() {
        if value.is_empty() {
            break;
        }
        let length: usize = value[1..].parse().unwrap();
        let mut command = vec![];
        for _ in 0..length {
            let x = lines.next().unwrap();
            println!("tokenizing {x}");

            let val = lines.next().unwrap();
            println!("tokenizing {val}");
            command.push(val);
        }
        result.push(command);
    }
    return result;
}

fn parse_command(input: &Vec<&str>) -> Option<Command> {
    match input.as_slice() {
        ["PING"] => { Some(PING) }
        _ => None,
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
        assert_eq!(parse_commands(input), vec![PING]);
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
