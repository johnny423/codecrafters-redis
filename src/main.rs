use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use anyhow::{Context, Result};
use db::{DB, Entry};

mod db;
mod command;
mod parse;

use clap::Parser;


#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "6379")]
    port: String,

}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    start_server(&args.port).await;
}

async fn start_server(port: &str) {
    let addr = format!("127.0.0.1:{port}");
    let listener = TcpListener::bind(addr).await.unwrap();
    let db: DB = Arc::new(Mutex::new(HashMap::<String, Entry>::new()));
    println!("Server listening on port {port}");

    while let Ok((stream, _)) = listener.accept().await {
        let db = db.clone();
        tokio::spawn(async move {
            if let Err(err) = handle_client(stream, db).await {
                eprintln!("Connection failed with: {err}")
            };
        });
    }
}

async fn handle_client(mut stream: TcpStream, db: DB) -> Result<()> {
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
                match parse::parse_commands(data) {
                    Ok(commands) => {
                        for command in commands {
                            println!("DEBUG: got command: {command:?}");
                            // todo: better response buffer
                            let response = command.handle(&db);

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

