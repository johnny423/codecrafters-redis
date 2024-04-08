use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};


#[derive(Debug, Ord, PartialOrd, PartialEq, Eq)]
pub enum Command {
    PING,
    // Set { key: String, value: String },
    // Get { key: String },
}

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").await.unwrap();

    println!("Server listening on port 6379");

    while let Ok((stream, _)) = listener.accept().await {
        tokio::spawn(async move {
            handle_client(stream).await;
        });
    }
}

async fn handle_client(mut stream: TcpStream) {
    println!("Client connected: {}", stream.peer_addr().unwrap());

    let (mut reader, mut writer) = stream.split();

    let mut buf = [0; 1024];
    loop {
        match reader.read(&mut buf).await {
            Ok(0) => {
                println!("Client disconnected: {}", stream.peer_addr().unwrap());
                return;
            }
            Ok(n) => {
                let data: String = String::from_utf8(buf.to_vec()).unwrap();
                let data = data.trim_end_matches('\0');
                for command in parse_commands(data) {
                    match command {
                        Command::PING => {
                            writer.write_all(b"+PONG\r\n").await.unwrap();
                        }
                    }
                }
            }
            Err(err) => {
                eprintln!("Error reading from socket: {}", err);
                return;
            }
        }
    }
}

fn parse_commands(data: &str) -> Vec<Command> {
    tokenize(&data).iter()
        .filter_map(
            |t| parse_command(&t)
        )
        .collect()
}


fn tokenize(input: &str) -> Vec<Vec<&str>> {
    let mut lines = input.lines();
    let mut result = vec![];
    while let Some(value) = lines.next() {
        println!("array length {value:?}");
        if value.is_empty() {
            break;
        }
        let length: usize = value[1..].parse().unwrap();
        let mut command = vec![];
        for _ in 0..length {
            let x = lines.next().unwrap();
            println!("str length {x}");

            let val = lines.next().unwrap();
            println!("tokenizing {val}");
            command.push(val);
        }
        result.push(command);
    }
    return result;
}

fn parse_command(input: &Vec<&str>) -> Option<Command> {
    match input.as_slice() {
        ["ping"] | ["PING"] => { Some(Command::PING) }
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(tokenize(input), vec![vec!["PING"]]);
    }

    #[test]
    fn test_parse_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(parse_commands(input), vec![Command::PING]);
    }

    #[test]
    fn test_full() {
        let input = "*3\r\n$3\r\nset\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nget\r\n$3\r\nfoo\r\n";
        assert_eq!(tokenize(input), vec![
            vec!["set", "foo", "bar"],
            vec!["get", "foo"],
        ]);
    }
}


//         println!("got new stream");
//         let mut reader = BufReader::new(&reader);
//
//         let mut buf = [0; 512];
//         let n = reader.read(&mut buf).unwrap();
//         if n == 0 {
//             break;
//         }
//         println!("got {n} bytes {buf:?}");
//         let data = String::from_utf8(buf.to_vec())?;
//
//         println!("got {data:?}");
//         let commands: Vec<Command> = parse_commands(&data.trim_end_matches('\0'));
//         for command in commands {
//             match command {
//                 PING => { stream.write_all(b"+PONG\r\n")?; }
//             }
//         }
//     }
// }
// );
//     println!("Client connected: {}", stream.peer_addr().unwrap());
//
//     let (mut reader, mut writer) = stream.split();
//
//     // Echo messages back to the client
//     let mut buf = [0; 1024];
//     loop {
//         match reader.read(&mut buf).await {
//             Ok(0) => {
//                 println!("Client disconnected: {}", stream.peer_addr().unwrap());
//                 return;
//             }
//             Ok(n) => {
//                 if let Err(err) = writer.write_all(&buf[..n]).await {
//                     eprintln!("Error writing to socket: {}", err);
//                     return;
//                 }
//             }
//             Err(err) => {
//                 eprintln!("Error reading from socket: {}", err);
//                 return;
//             }
//         }
//     }
// }
//
// fn main() -> Result<()> {
//     // You can use print statements as follows for debugging, they'll be visible when running tests.
//     let address = "127.0.0.1:6379";
//     let listener = TcpListener::bind(address).with_context(
//         || format!("tcp bind to {address}")
//     )?;
//     println!("listening to {address}");
//
//
//     for stream in listener.incoming() {
//         match stream {
//             Ok(mut stream) => {
//                 thread::spawn(move ||
//                     {
//                         loop {
//                             println!("got new stream");
//                             let mut reader = BufReader::new(&stream);
//
//                             let mut buf = [0; 512];
//                             let n = reader.read(&mut buf).unwrap();
//                             if n == 0 {
//                                 break;
//                             }
//                             println!("got {n} bytes {buf:?}");
//                             let data = String::from_utf8(buf.to_vec())?;
//
//                             println!("got {data:?}");
//                             let commands: Vec<Command> = parse_commands(&data.trim_end_matches('\0'));
//                             for command in commands {
//                                 match command {
//                                     PING => { stream.write_all(b"+PONG\r\n")?; }
//                                 }
//                             }
//                         }
//                     }
//                 );
//             }
//             Err(e) => {
//                 println!("error: {}", e);
//             }
//         }
//     }
//     Ok(())
// }
