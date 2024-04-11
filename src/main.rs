use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use anyhow::Result;
use clap::{Arg, Command as ClapCommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;

use db::{DB, Entry};

use crate::command::{Command, parse_command};
use crate::parse::*;

mod db;
mod command;
mod parse;


const EMPTY: &[u8] = b"524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2";
const PONG: &[u8] = b"+PONG\r\n";
const OK: &[u8] = b"+OK\r\n";
const ERR: &[u8] = b"-ERR\r\n";

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


type Tx = mpsc::UnboundedSender<String>;
// type Rx = mpsc::UnboundedReceiver<String>;

struct Router {
    peers: HashMap<SocketAddr, Tx>,
    replicas: HashSet<SocketAddr>,
}

impl Router {
    fn new() -> Self {
        Router {
            peers: HashMap::new(),
            replicas: HashSet::<SocketAddr>::new(),
        }
    }

    fn broadcast_to_replicas(&mut self, message: &str) {
        for replica in self.replicas {
            if let Some(peer) = self.peers.get(&replica) {
                let _ = peer.send(message.into());
            }
        }
    }

    fn register_peer(&mut self, receiver: SocketAddr, tx: Tx) {
        self.peers.insert(receiver, tx);
    }

    fn remove_peer(&mut self, receiver: &SocketAddr){
        self.peers.remove(receiver);
        self.replicas.remove(receiver);
    }

    fn register_replica(&mut self, replica: SocketAddr) {
        self.replicas.insert(replica);
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
    let db: DB = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));
    let server = Arc::new(server);
    let router = Arc::new(Mutex::new(Router::new()));


    println!("Server listening on port :{port}", port = server.port);

    if let Role::Replica { host, port } = &server.role {
        let master_addr = format!("{host}:{port}", );
        let server = server.clone();
        tokio::spawn(
            sync_with_master(master_addr, server)
        );
    }

    let addr = format!("127.0.0.1:{port}", port = server.port);
    let listener = TcpListener::bind(addr).await.unwrap();

    while let Ok((stream, peer)) = listener.accept().await {
        println!("Client connected: {}", peer);

        let db = db.clone();
        let server = server.clone();
        let router = Arc::clone(&router);

        tokio::spawn(
            client(stream, peer, db, server, router)
        );
    }
}

async fn client(
    mut stream: TcpStream,
    peer: SocketAddr,
    db: DB,
    server: Arc<Server>,
    router: Arc<Mutex<Router>>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    router.lock().unwrap().register_peer(peer, tx.clone());

    let mut buf = [0; 1024];
    loop {
        tokio::select! {
            // A message was received from a peer. Send it to the current user.
            Some(msg) = rx.recv() => {
                // todo handle
                let _ = stream.write_all(msg.as_ref()).await;
            }
            result = stream.read(&mut buf)  =>  match result {
                Ok(n) => {
                    if n == 0{
                        break
                    }
                    // todo: handle
                    let data: String = String::from_utf8(buf.to_vec()).unwrap();
                    let data = data.trim_end_matches('\0');

                    // parse requests into commands
                    let command = parse_command(data);

                    println!("DEBUG: got command: {command:?}");
                    execute(&command, &mut stream, &peer, &db, &server, &router).await.unwrap();
                }
                Err(_err) =>{
                    break
                }
            }
        }
    }

    let mut guard = router.lock().unwrap();
    guard.remove_peer(&peer);
}


async fn sync_with_master(master_addr: String, server: Arc<Server>) {
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


async fn execute(
    command: &Command,
    stream: &mut TcpStream,
    peer: &SocketAddr,
    db: &DB,
    server: &Arc<Server>,
    router: &Arc<Mutex<Router>>,
) -> Result<()> {
    match &command {
        Command::Ping => {
            stream.write_all(PONG).await?;
        }
        Command::Echo(value) => {
            stream.write_all(bulk_string(Some(value)).as_ref()).await?;
        }
        Command::Get { key } => {
            let val = bulk_string(db::get(db, key).as_deref());
            stream.write_all(val.as_ref()).await?;
        }
        Command::Set { key, value, ex } => {
            db::set(db, key.to_owned(), value.to_string(), ex.to_owned());
            stream.write_all(OK).await?;

            {
                let msg = array(&vec!["set", key, value]);
                let mut guard = router.lock().unwrap();
                guard.broadcast_to_replicas(&msg)
            }
        }
        Command::Info => {
            let val = pairs(server.info().into_iter());
            stream.write_all(val.as_ref()).await?;
        }
        Command::Replconf => {
            // todo: save info
            stream.write_all(OK).await?;
        }
        Command::Psync => {
            let val = format!(
                "+FULLRESYNC {repl_id} {offset}\r\n",
                repl_id = server.replid(),
                offset = server.offset()
            );
            stream.write_all(val.as_ref()).await?;

            let empty = hex::decode(EMPTY)
                .unwrap();

            let val = format!("${}\r\n", empty.len());
            stream.write_all(val.as_ref()).await?;
            stream.write_all(&empty).await?;

            let mut guard = router.lock().unwrap();
            guard.register_replica(*peer);
        }
        Command::Err => {
            stream.write_all(ERR).await?;
        }
    };
    Ok(())
}

fn send_to_replicas(router: &Arc<Mutex<Router>>, replicas: &Arc<Mutex<HashSet<SocketAddr>>>, msg: &String) {
    let replicas_guard = replicas.lock().unwrap();
    let replicas = replicas_guard.clone();
    drop(replicas_guard);

    for replica in replicas.iter() {
        router.lock().unwrap().send_to(replica, &msg)
    }
}
