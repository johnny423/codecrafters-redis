use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Context, Result};
use db::{DB, Entry, get};
use clap::{Arg, Command as ClapCommand};
use crate::command::{Command, parse_command};
use crate::parse::{bulk_string, pairs};
use std::{fmt::Write};

mod db;
mod command;
mod parse;

#[allow(dead_code)]
#[derive(Debug)]
enum Role {
    Master,
    Replica {
        host: String,
        port: String,
    },
}

#[derive(Debug)]
struct Server {
    port: String,
    role: Role,
}

impl Server {
    pub fn new(port: String, role: Role) -> Self {
        Self {
            port,
            role,
        }
    }
    pub fn replid(&self) -> &str {
        "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
    }
    pub fn offset(&self) -> &str {
        "0"
    }

    pub fn info(&self) -> Vec<(&str, &str)> {
        let mut result = vec![];
        let role = match self.role {
            Role::Master => { "master" }
            Role::Replica { .. } => { "slave" }
        };

        result.push(("role", role));
        result.push(("master_replid", self.replid()));
        result.push(("master_repl_offset", self.offset()));

        result
    }
}

#[tokio::main]
async fn main() {
    let matches = ClapCommand::new("App Command Parser")
        .version("1.0")
        .author("Your Name")
        .about("Parses app command with port and optional replicaof")
        .arg(
            Arg::new("port")
                .short('p')
                .long("port")
                .value_name("PORT")
                .help("Sets the port number")
                .required(false)
        )
        .arg(
            Arg::new("replicaof")
                .long("replicaof")
                .value_names(["MASTER_HOST", "MASTER_PORT"])
                .help("Sets the master host and port for replication")
                .required(false),
        )
        .get_matches();

    let port = matches.get_one::<String>("port").map_or(
        "6379".to_string(), |v| v.clone(),
    );
    let role = match matches.get_many::<String>("replicaof") {
        Some(mut values) => {
            Role::Replica {
                host: values.next().unwrap().clone(),
                port: values.next().unwrap().clone(),
            }
        }
        None => { Role::Master }
    };

    let server = Server::new(
        port,
        role,
    );


    start_server(server).await;
}

async fn start_server(server: Server) {
    let addr = format!("127.0.0.1:{port}", port = server.port);
    let listener = TcpListener::bind(addr).await.unwrap();
    let db: DB = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));
    println!("Server listening on port :{port}", port = server.port);

    if let Role::Replica { host, port } = &server.role {
        let master_addr = format!("{host}:{port}");
        println!("Connecting to master at {master_addr}... ");
        let mut stream = TcpStream::connect(master_addr.clone()).await.unwrap();

        println!("Connected to master! starting handshake... ");
        stream.write_all(
            parse::array(&vec!["ping"]).as_bytes()
        ).await.unwrap();

        let mut buf = [0; 1024];
        stream.read(&mut buf).await.unwrap();
        // todo check pong

        stream.write_all(
            parse::array(&vec!["REPLCONF", "listening-port", &server.port]).as_bytes()
        ).await.unwrap();
        stream.read(&mut buf).await.unwrap();
        // todo check ok

        stream.write_all(
            parse::array(&vec!["REPLCONF", "capa", "psync2"]).as_bytes()
        ).await.unwrap();
        stream.read(&mut buf).await.unwrap();

        stream.write_all(
            parse::array(&vec!["PSYNC", "?", "-1"]).as_bytes()
        ).await.unwrap();
        stream.read(&mut buf).await.unwrap();
        // todo check ok
    }

    let server = Arc::new(server);
    while let Ok((stream, _)) = listener.accept().await {
        let db = db.clone();
        let server = server.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, db, server).await {
                eprintln!("Connection failed with: {err}")
            };
        });
    }
}


const EMPTY_FILE: &'static [u8] = b"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";

async fn handle_client(mut stream: TcpStream, db: DB, server: Arc<Server>) -> Result<()> {
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
                let command = parse_command(data);

                println!("DEBUG: got command: {command:?}");
                match &command {
                    Command::Ping => {
                        writer.write_all(b"+PONG\r\n").await?;
                    }
                    Command::Echo(value) => {
                        writer.write_all(bulk_string(Some(value)).as_ref()).await?;
                    }
                    Command::Get { key } => {
                        let val = bulk_string(get(&db, key).as_deref());
                        writer.write_all(val.as_ref()).await?;
                    }
                    Command::Set { key, value, ex } => {
                        db::set(&db, key.to_owned(), value.to_string(), ex.to_owned());
                        writer.write_all(b"+OK\r\n").await?;
                    }
                    Command::Info => {
                        let val = pairs(server.info().into_iter());
                        writer.write_all(val.as_ref()).await?;
                    }
                    Command::Replconf => {
                        writer.write_all(b"+OK\r\n").await?;
                    }
                    Command::Psync => {
                        let val = format!(
                            "+FULLRESYNC {repl_id} {offset}\r\n",
                            repl_id = server.replid(), offset = server.offset()
                        );
                        writer.write_all(val.as_ref()).await?;

                        let empty = encode_hex(EMPTY_FILE);
                        let val = format!("${}\r\n{}", empty.len(), empty);
                        writer.write_all(val.as_ref()).await?;
                    }
                    Command::Err => {
                        writer.write_all(b"-ERR\r\n").await?;
                    }
                };
            }
            Err(err) => {
                eprintln!("Error reading from socket: {}", err);
                return Ok(());
            }
        }
    }
}


pub fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        write!(&mut s, "{:02x}", b).unwrap();
    }
    s
}