use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use tokio::io::{AsyncWriteExt, BufReader};
use tokio::net::tcp::WriteHalf;
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::{db, EMPTY, ERR, OK, PONG, Router, Server};
use crate::command::{Command, Replconf};
use crate::db::DB;
use crate::parse::{array, bulk_string, pairs, tokenize};

pub async fn client_handler(
    mut stream: TcpStream,
    peer: SocketAddr,
    db: DB,
    server: Arc<Server>,
    router: Arc<Mutex<Router>>,
) {
    let (tx, mut rx) = mpsc::unbounded_channel::<String>();
    router.lock().unwrap().register_peer(peer, tx.clone());

    let (mut reader, mut writer) = stream.split();
    let mut reader = BufReader::new(&mut reader);
    loop {
        tokio::select! {
            // A message was received from a peer. Send it to the current user.
            Some(msg) = rx.recv() => {
                // todo handle
                let _ = writer.write_all(msg.as_ref()).await;
            }
            result = tokenize(&mut reader) =>  {
                match result {
                    Ok(None) | Err(_) => {
                        break
                    }
                    Ok(Some((arr, _count))) => {
                        // todo: handle
                        let command=  Command::parse(&arr);
                        handle_client_command(&command, &mut writer, &peer, &db, &server, &router).await.unwrap();
                    }
                }
            }
        }
    }

    let mut guard = router.lock().unwrap();
    guard.remove_peer(&peer);
}


async fn handle_client_command(
    command: &Command,
    stream: &mut WriteHalf<'_>,
    peer: &SocketAddr,
    db: &DB,
    server: &Arc<Server>,
    router: &Arc<Mutex<Router>>,
) -> anyhow::Result<()> {
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
        Command::Replconf(internal) => {
            // todo: save info
            if let Replconf::ListeningPort(_) | Replconf::Capa(_) = internal {
                stream.write_all(OK).await?;
            }
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
            println!("Master: finish sending file");

            let mut guard = router.lock().unwrap();
            guard.register_replica(*peer);
        }
        Command::Wait => {
            stream.write_all(b":0\r\n").await?;
        }
        Command::Err => {
            stream.write_all(ERR).await?;
        }
    };
    Ok(())
}
