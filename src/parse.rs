pub fn tokenize(input: &str) -> Vec<Vec<&str>> {
    let mut lines = input.lines();
    let mut result = vec![];
    while let Some(value) = lines.next() {
        if value.is_empty() || !value.starts_with('*') {
            break;
        }

        let length = match value[1..].parse::<usize>() {
            Ok(size) => size,
            Err(_) => break, // todo log error
        };
        let mut command = vec![];
        for i in 0..length {
            if lines.next().is_none() {
                eprintln!("ERROR: skipping value length at index {i}, for input: {input:?}");
                break;
            }

            let val = match lines.next() {
                None => {
                    eprintln!("ERROR: parsing value at index {i}, for input: {input:?}");
                    break;
                }
                Some(v) => v,
            };

            command.push(val);
        }
        result.push(command);
    }
    result
}

pub fn pairs<'a>(pairs: impl ExactSizeIterator<Item=(&'a str, &'a str)>) -> String {
    let mut result = format!("*{len}\r\n", len = pairs.len());
    for (key, value) in pairs {
        let a = bulk_string(Some(format!("{key}:{value}")));
        result += &a;
    }
    result
}

pub fn bulk_string(string: Option<String>) -> String {
    match string {
        None => "$-1\r\n".to_string(),
        Some(value) => {
            format!("${}\r\n{value}\r\n", value.len())
        }
    }
}


#[cfg(test)]
mod test {
    use crate::command::parse_commands;
    use crate::parse::tokenize;
    use super::*;

    #[test]
    fn test_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(tokenize(input), vec![vec!["PING"]]);
    }

    #[test]
    fn test_parse_ping() {
        let input = "*1\r\n$4\r\nPING\r\n";
        assert_eq!(parse_commands(input).unwrap(), vec![Command::Ping]);
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
