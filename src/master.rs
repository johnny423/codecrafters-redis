use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, RwLock};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::{ReadHalf, WriteHalf};
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::{select, time};

use crate::command::{Command, Replconf};
use crate::db::DB;
use crate::parse::{array, bulk_string, pairs, tokenize};
use crate::{Server, EMPTY, ERR, OK, PONG};

type Tx = mpsc::UnboundedSender<String>;

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

#[derive(Clone)]
pub struct Replicas {
    peers: Arc<RwLock<HashMap<SocketAddr, Replica>>>,
}

impl Replicas {
    pub fn new() -> Self {
        Self {
            peers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn broadcast(&mut self, msg: &str) {
        // read lock only
        for (_, replica) in self.peers.read().unwrap().iter() {
            replica.send(msg.to_owned())
        }
    }

    pub fn len(&self) -> usize {
        self.peers.read().unwrap().len()
    }

    fn add(&mut self, peer: &Peer) {
        let peer = peer.clone();
        // write lock
        self.peers
            .write()
            .unwrap()
            .insert(peer.addr, Replica::new(peer));
    }

    pub fn remove(&mut self, addr: &SocketAddr) {
        // write lock
        self.peers.write().unwrap().remove(addr);
    }
}

enum PeerType {
    Client,
    Replica {
        offset: usize,
        interval: time::Interval,
    },
}

struct MasterConnection {
    internal: PeerType,
    rx: UnboundedReceiver<String>,
    peer: Peer,
    db: DB,
    server: Arc<Server>,
    replicas: Replicas,
}

impl MasterConnection {
    async fn handle(
        mut self,
        reader: &mut BufReader<&mut ReadHalf<'_>>,
        writer: &mut WriteHalf<'_>,
    ) -> Option<Self> {
        match self.internal {
            PeerType::Client => {
                let result = tokenize(reader).await;
                match result {
                    Ok(None) | Err(_) => None,
                    Ok(Some((arr, _count))) => {
                        let command = Command::parse(&arr);
                        let next = self.handle_client_command(command, writer).await.unwrap();
                        Some(next)
                    }
                }
            }
            PeerType::Replica {
                mut offset,
                mut interval,
            } => {
                select! {
                        // A message was received from a peer. Send it to the current user.
                        Some(msg) = self.rx.recv() => {
                            // get send messages
                            let msg: &[u8] = msg.as_ref();
                            offset += msg.len();
                            if writer.write_all(msg).await.is_err(){
                                return None;
                            }
                        }
                        _ = interval.tick() => {
                            let msg = array(&vec!["replconf", "getack", "*"]);
                            let msg: &[u8] = msg.as_ref();
                            offset  += msg.len();
                            if  writer.write_all(msg).await.is_err(){
                                return None;
                            }
                            if let Ok(Some((arr, _))) = tokenize(reader).await{
                                let command = Command::parse(&arr);
                                match command {
                                    Command::Replconf(Replconf::Ack(_)) => {
                                        println!("got ack from replica sending to channel ");
                                    },
                                    _ => {
                                        return None
                                    }
                                }
                            }

                        }
                }
                self.internal = PeerType::Replica { offset, interval };
                Some(self)
            }
        }
    }

    async fn handle_client_command(
        mut self,
        command: Command,
        stream: &mut WriteHalf<'_>,
    ) -> anyhow::Result<Self> {
        match &command {
            Command::Ping => {
                stream.write_all(PONG).await?;
            }
            Command::Echo(value) => {
                stream.write_all(bulk_string(Some(value)).as_ref()).await?;
            }
            Command::Get { key } => {
                let val = bulk_string(self.db.get(key).as_deref());
                stream.write_all(val.as_ref()).await?;
            }
            Command::Set { key, value, ex } => {
                self.db
                    .set(key.to_owned(), value.to_string(), ex.to_owned());
                stream.write_all(OK).await?;

                let msg = array(&vec!["set", key, value]);
                self.replicas.broadcast(&msg);
            }
            Command::Info => {
                let val = pairs(self.server.info().into_iter());
                stream.write_all(val.as_ref()).await?;
            }
            Command::Replconf(Replconf::ListeningPort(_) | Replconf::Capa(_)) => {
                // todo: save info
                stream.write_all(OK).await?;
            }
            Command::Psync => {
                let val = format!(
                    "+FULLRESYNC {repl_id} {offset}\r\n",
                    repl_id = self.server.replid(),
                    offset = self.server.offset()
                );
                stream.write_all(val.as_ref()).await?;

                let empty = hex::decode(EMPTY).unwrap();
                let val = format!("${}\r\n", empty.len());
                stream.write_all(val.as_ref()).await?;
                stream.write_all(&empty).await?;
                println!("Master: finish sending file");

                self.replicas.add(&self.peer);
                self.internal = PeerType::Replica {
                    offset: 0,
                    interval: time::interval(time::Duration::from_millis(500)),
                };
                return Ok(self);
            }
            Command::Wait(_reps, _timeout) => {
                let count = self.replicas.len();
                stream
                    .write_all(format!(":{}\r\n", count).as_bytes())
                    .await?;
            }
            Command::Err => {
                stream.write_all(ERR).await?;
            }
            _ => {}
        };
        Ok(self)
    }
}

pub async fn client_handler(
    mut stream: TcpStream,
    peer_addr: SocketAddr,
    db: DB,
    server: Arc<Server>,
    mut replicas: Replicas,
) {
    let (tx, rx) = mpsc::unbounded_channel::<String>();
    let peer = Peer {
        addr: peer_addr,
        tx,
    };

    let (mut reader, mut writer) = stream.split();
    let mut reader = BufReader::new(&mut reader);
    let mut master = Some(MasterConnection {
        internal: PeerType::Client,
        peer: peer.clone(),
        replicas: replicas.clone(),
        rx,
        db,
        server,
    });

    while let Some(x) = master {
        master = x.handle(&mut reader, &mut writer).await;
    }

    println!("client disconnected {}", peer.addr);
    replicas.remove(&peer.addr);
}
