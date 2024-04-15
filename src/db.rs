use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum Entry {
    Simple(String),
    Expire(String, Instant),
}

pub struct DB(Arc<Mutex<HashMap<String, Entry>>>);

impl DB {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(HashMap::new())))
    }
    pub fn get(&self, key: &str) -> Option<String> {
        let guard = self.0.lock().unwrap();
        match guard.get(key) {
            None => None,
            Some(Entry::Simple(value)) => Some(value.clone()),
            Some(Entry::Expire(value, ex)) => {
                if &Instant::now() > ex {
                    None
                } else {
                    Some(value.clone())
                }
            }
        }
    }

    pub fn set(&self, key: String, value: String, ex: Option<Duration>) {
        let entry = match ex {
            None => Entry::Simple(value),
            Some(duration) => Entry::Expire(value, Instant::now() + duration),
        };
        self.0.lock().unwrap().insert(key, entry);
    }
}

impl Clone for DB {
    fn clone(&self) -> Self {
        DB(self.0.clone())
    }
}
