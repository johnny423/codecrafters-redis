use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time;

use crate::command::{Command, Replconf};
use crate::db::DB;
use crate::parse::{array, bulk_string, pairs, tokenize};
use crate::{Server, EMPTY, ERR, OK, PONG};

type Tx = mpsc::UnboundedSender<String>;
// type Rx = mpsc::UnboundedReceiver<String>;

#[derive(Clone)]
struct Peer {
    addr: SocketAddr,
    tx: Tx,
}

struct Replica(Arc<Mutex<Peer>>);

impl Replica {
    pub fn new(peer: Peer) -> Self {
        Self(Arc::new(Mutex::new(peer)))
    }

    pub fn send(&self, val: String) {
        self.0.lock().unwrap().tx.send(val).unwrap()
    }
}

pub struct Replicas(Arc<RwLock<HashMap<SocketAddr, Replica>>>);

impl Replicas {
    pub fn new() -> Self {
        Self(Arc::new(RwLock::new(HashMap::new())))
    }

    pub fn broadcast(&self, msg: &str) {
        // read lock only
        for (_, replica) in self.0.read().unwrap().iter() {
            replica.send(msg.to_owned())
        }
    }

    pub fn len(&self) -> usize {
        self.0.read().unwrap().len()
    }

    fn add(&mut self, peer: &Peer) {
        let peer = peer.clone();
        // write lock
        self.0
            .write()
            .unwrap()
            .insert(peer.addr, Replica::new(peer));
    }

    pub fn remove(&mut self, addr: &SocketAddr) {
        // write lock
        self.0.write().unwrap().remove(addr);
    }
}

impl Clone for Replicas {
    fn clone(&self) -> Self {
        Self(Arc::clone(&self.0))
    }
}

enum PeerType {
    Client,
    Replica,
}

pub async fn client_handler(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    db: DB,
    server: Arc<Server>,
    mut replicas: Replicas,
) {
    let mut peer_type: PeerType = PeerType::Client;
    let mut interval = time::interval(time::Duration::from_millis(100));
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    let peer = Peer {
        addr: peer_addr,
        tx,
    };

    let (mut reader, mut writer) = stream.split();
    let mut reader = BufReader::new(&mut reader);
    loop {
        match peer_type {
            PeerType::Client => {
                let result = tokenize(&mut reader).await;
                match result {
                    Ok(None) | Err(_) => {
                        break;
                    }
                    Ok(Some((arr, _count))) => {
                        let command = Command::parse(&arr);
                        handle_client_command(
                            &command,
                            &mut writer,
                            &peer,
                            &db,
                            &server,
                            &mut replicas,
                            &mut peer_type,
                        )
                        .await
                        .unwrap();
                    }
                }
            }
            PeerType::Replica => {
                tokio::select! {
                    // A message was received from a peer. Send it to the current user.
                    Some(msg) = rx.recv() => {
                        // todo handle
                        let _ = writer.write_all(msg.as_ref()).await;
                    }
                    _ = &mut Box::pin(interval.tick()) => {
                        let msg = array(&vec!["replconf", "getack", "*"]);
                        let _ = writer.write_all(msg.as_ref()).await;
                        if let Ok(Some((arr, _))) = tokenize(&mut reader).await{
                            let command = Command::parse(&arr);
                            if let Command::Replconf(Replconf::Ack(val)) = command{
                                println!("master got ack: {val}");
                            }
                        }
                    },
                }
            }
        }
    }

    replicas.remove(&peer.addr);
}

async fn handle_client_command(
    command: &Command,
    stream: &mut WriteHalf<'_>,
    peer: &Peer,
    db: &DB,
    server: &Arc<Server>,
    replicas: &mut Replicas,
    peer_type: &mut PeerType,
) -> anyhow::Result<()> {
    match &command {
        Command::Ping => {
            stream.write_all(PONG).await?;
        }
        Command::Echo(value) => {
            stream.write_all(bulk_string(Some(value)).as_ref()).await?;
        }
        Command::Get { key } => {
            let val = bulk_string(db.get(key).as_deref());
            stream.write_all(val.as_ref()).await?;
        }
        Command::Set { key, value, ex } => {
            db.set(key.to_owned(), value.to_string(), ex.to_owned());
            stream.write_all(OK).await?;

            let msg = array(&vec!["set", key, value]);
            replicas.broadcast(&msg);
        }
        Command::Info => {
            let val = pairs(server.info().into_iter());
            stream.write_all(val.as_ref()).await?;
        }
        Command::Replconf(Replconf::ListeningPort(_) | Replconf::Capa(_)) => {
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

            let empty = hex::decode(EMPTY).unwrap();

            let val = format!("${}\r\n", empty.len());
            stream.write_all(val.as_ref()).await?;
            stream.write_all(&empty).await?;
            println!("Master: finish sending file");

            replicas.add(peer);
            *peer_type = PeerType::Replica;
        }
        Command::Wait => {
            let count = replicas.len();
            stream
                .write_all(format!(":{}\r\n", count).as_bytes())
                .await?;
        }
        Command::Err => {
            stream.write_all(ERR).await?;
        }
        _ => {}
    };
    Ok(())
}
