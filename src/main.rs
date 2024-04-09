use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Context, Result};
use db::{DB, Entry};
use clap::{Arg, Command};

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

    pub fn info(&self) -> Vec<(&str, &str)> {
        let mut result = vec![];
        let role = match self.role {
            Role::Master => { "master" }
            Role::Replica { .. } => { "slave" }
        };

        result.push(("role", role));
        result.push(("master_replid", "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"));
        result.push(("master_repl_offset", "0"));

        result
    }
}

#[tokio::main]
async fn main() {
    let matches = Command::new("App Command Parser")
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
                match command::parse_commands(data) {
                    Ok(commands) => {
                        for command in commands {
                            println!("DEBUG: got command: {command:?}");
                            // todo: better response buffer
                            let response = command.handle(&db, &server);

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

