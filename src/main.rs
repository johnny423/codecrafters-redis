use std::sync::Arc;

use clap::{Arg, Command as ClapCommand};
use tokio::net::{TcpListener, TcpStream};

use db::DB;

use crate::master::Replicas;
use crate::replica::sync_with_master;

mod command;
mod db;
mod master;
mod parse;
mod replica;

const EMPTY: &[u8] = b"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
const PONG: &[u8] = b"+PONG\r\n";
const OK: &[u8] = b"+OK\r\n";
const ERR: &[u8] = b"-ERR\r\n";

#[allow(dead_code)]
#[derive(Debug)]
enum Role {
    Master,
    Replica { host: String, port: String },
}

#[derive(Debug)]
struct Server {
    port: String,
    role: Role,
}

impl Server {
    pub fn new(port: String, role: Role) -> Self {
        Self { port, role }
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
            Role::Master => "master",
            Role::Replica { .. } => "slave",
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
                .required(false),
        )
        .arg(
            Arg::new("replicaof")
                .long("replicaof")
                .value_names(["MASTER_HOST", "MASTER_PORT"])
                .help("Sets the master host and port for replication")
                .required(false),
        )
        .get_matches();

    let port = matches
        .get_one::<String>("port")
        .map_or("6379".to_string(), |v| v.clone());
    let role = match matches.get_many::<String>("replicaof") {
        Some(mut values) => Role::Replica {
            host: values.next().unwrap().clone(),
            port: values.next().unwrap().clone(),
        },
        None => Role::Master,
    };

    let server = Server::new(port, role);

    start_server(server).await;
}

async fn start_server(server: Server) {
    let db = DB::new();
    let server = Arc::new(server);
    let replicas = Replicas::new();

    if let Role::Replica { host, port } = &server.role {
        let master_addr = format!("{host}:{port}",);
        let server = server.clone();
        let db = db.clone();
        tokio::spawn(async move {
            let stream = TcpStream::connect(master_addr.clone()).await.unwrap();
            if let Err(err) = sync_with_master(stream, server, db).await {
                eprintln!("[ERROR] Replica: Disconnected from master with error: {err}")
            } else {
                eprintln!("[INFO] Replica: Disconnected from master")
            }
        });
    }

    let addr = format!("127.0.0.1:{port}", port = server.port);
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server listening on port :{port}", port = server.port);

    while let Ok((stream, peer)) = listener.accept().await {
        println!("Client connected: {}", peer);

        let db = db.clone();
        let server = server.clone();
        let replicas = replicas.clone();

        tokio::spawn(master::client_handler(stream, peer, db, server, replicas));
    }
}
