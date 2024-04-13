use std::sync::Arc;

use anyhow::{anyhow, Result};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

use crate::{db, parse, Server};
use crate::command::{Command, Replconf};
use crate::db::DB;
use crate::parse::array;

pub async fn sync_with_master(mut stream: TcpStream, server: Arc<Server>, db: DB) -> Result<()> {
    let (mut reader, mut writer) = stream.split();
    let mut reader = BufReader::new(&mut reader);
    let mut response = String::new();

    // Handshake
    // Ping
    writer.write_all(array(&vec!["ping"]).as_bytes()).await?;
    response.clear();
    reader.read_line(&mut response).await?;
    if response.to_lowercase() != "+pong\r\n".to_lowercase() {
        return Err(anyhow!("expected pong, but got: {response}"));
    }

    // ConfPort
    writer.write_all(
        array(&vec!["REPLCONF", "listening-port", &server.port]).as_bytes()
    ).await?;
    response.clear();
    reader.read_line(&mut response).await?;
    if response.to_lowercase() != "+ok\r\n".to_lowercase() {
        return Err(anyhow!("expected ok, but got: {response:?}"));
    }

    // ConfFormat
    writer.write_all(
        array(&vec!["REPLCONF", "capa", "psync2"]).as_bytes()
    ).await?;
    response.clear();
    reader.read_line(&mut response).await?;
    if response.to_lowercase() != "+ok\r\n".to_lowercase() {
        return Err(anyhow!("expected ok, but got: {response:?}"));
    }

    // SyncFile
    response.clear();
    writer.write_all(
        array(&vec!["PSYNC", "?", "-1"]).as_bytes()
    ).await?;
    reader.read_line(&mut response).await?;
    // todo assert response

    // read file length
    response.clear();
    reader.read_line(&mut response).await?;
    println!("file length {:?}", response);
    let file_length = response[1..response.len() - 2].parse()?;

    // read file
    let mut file_buff = vec![0; file_length];
    println!("file buff {:?}", file_buff);
    let _ = reader.read_exact(&mut file_buff).await;

    // Handshake ended now wait for commands
    while let Some(tokenz) = parse::tokenize(&mut reader).await? {
        let command = Command::parse(&tokenz);
        match command {
            Command::Set { key, value, ex } => {
                db::set(&db, key.to_owned(), value.to_string(), ex.to_owned());
                println!("Replica: wrote {key} {value}")
            }
            Command::Replconf(Replconf::GetAck(_val)) => {
                let response = array(&vec!["REPLCONF", "ACK", "0"]);
                writer.write_all(response.as_bytes()).await?;
            }
            _ => {}
        }
    }


    Ok(())
}
