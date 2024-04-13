use anyhow::anyhow;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::tcp::ReadHalf;

pub fn pairs<'a>(pairs: impl ExactSizeIterator<Item=(&'a str, &'a str)>) -> String {
    let mut result = String::new();
    for (key, value) in pairs {
        let a = format!("{key}:{value}\r\n");
        result += &a;
    }
    bulk_string(Some(&result))
}

pub fn bulk_string(string: Option<&str>) -> String {
    match string {
        None => "$-1\r\n".to_string(),
        Some(value) => {
            format!("${}\r\n{value}\r\n", value.len())
        }
    }
}

pub fn array(arr: &Vec<&str>) -> String {
    let mut result = format!("*{len}\r\n", len = arr.len());
    for val in arr {
        result += &bulk_string(Some(val));
    }
    result
}

pub async fn tokenize(input: &mut BufReader<&mut ReadHalf<'_>>) -> anyhow::Result<Option<Vec<String>>> {
    let mut response = String::new();
    let x = input.read_line(&mut response).await?;
    if x == 0 {
        return Ok(None);
    }

    if !response.starts_with('*') {
        return Err(anyhow!("Expected an array (starts with *) but got {response} "));
    }

    let length = match response[1..].strip_suffix("\r\n").expect("split by lines").parse::<usize>() {
        Ok(size) => size,
        Err(err) => return Err(anyhow!("Failed to parse size {err}")),
    };

    let mut array = vec![];
    for _ in 0..length {
        // read value size
        let mut response = String::new();
        let x = input.read_line(&mut response).await?;
        if x == 0 {
            return Err(anyhow!("EOF"));
        }

        let mut response = String::new();
        let x = input.read_line(&mut response).await?;
        if x == 0 {
            return Err(anyhow!("EOF"));
        }
        array.push(response.strip_suffix("\r\n").expect("split by lines").to_string());
    }
    Ok(Some(array))
}
