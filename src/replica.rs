use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::{db, parse, Server};
use crate::command::Command;
use crate::db::DB;
use crate::parse::array;

#[derive(Debug)]
enum Handshake {
    Ping,
    ConfPort,
    ConfFormat,
    SyncFile,
}

pub async fn sync_with_master(mut stream: TcpStream, server: Arc<Server>, db: DB) -> Result<()> {
    let mut state = Handshake::Ping;
    let (mut reader, mut writer) = stream.split();
    let mut reader = BufReader::new(&mut reader);
    let mut response = String::new();

    // Handshake
    loop {
        match state {
            Handshake::Ping => {
                writer.write_all(array(&vec!["ping"]).as_bytes()).await?;
                response.clear();
                reader.read_line(&mut response).await?;
                if response.to_lowercase() != "+pong\r\n".to_lowercase() {
                    return Err(anyhow!("expected pong, but got: {response}"));
                }
                state = Handshake::ConfPort;
            }
            Handshake::ConfPort => {
                writer.write_all(
                    array(&vec!["REPLCONF", "listening-port", &server.port]).as_bytes()
                ).await?;
                response.clear();
                reader.read_line(&mut response).await?;
                if response.to_lowercase() != "+ok\r\n".to_lowercase() {
                    return Err(anyhow!("expected ok, but got: {response:?}"));
                }
                state = Handshake::ConfFormat;
            }
            Handshake::ConfFormat => {
                response.clear();
                writer.write_all(
                    array(&vec!["REPLCONF", "capa", "psync2"]).as_bytes()
                ).await?;
                reader.read_line(&mut response).await?;
                if response.to_lowercase() != "+ok\r\n".to_lowercase() {
                    return Err(anyhow!("expected ok, but got: {response:?}"));
                }

                state = Handshake::SyncFile;
            }
            Handshake::SyncFile => {
                response.clear();
                writer.write_all(
                    array(&vec!["PSYNC", "?", "-1"]).as_bytes()
                ).await?;
                reader.read_line(&mut response).await?;

                response.clear();
                reader.read_line(&mut response).await?;

                println!("file length {:?}", response);
                let file_length = response[1..response.len() - 2].parse()?;
                let mut file_buff = vec![0; file_length];
                println!("file buff {:?}", file_buff);
                let _ = reader.read_exact(&mut file_buff).await;

                break;
            }
        }
    }


    // sync updates
    while let Some(tokenz) = parse::tokenize(&mut reader).await? {
        let command = Command::parse(&tokenz);
        if let Command::Set { key, value, ex } = command {
            db::set(&db, key.to_owned(), value.to_string(), ex.to_owned());
            println!("Replica: wrote {key} {value}")
        }
    }


    Ok(())
}
