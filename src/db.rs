use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

#[derive(Debug)]
pub enum Entry {
    Simple(String),
    Expire(String, Instant),
}

pub type DB = Arc<Mutex<HashMap<String, Entry>>>;


pub fn get(db: &DB, key: &str) -> Option<String> {
    let guard = db.lock().unwrap();
    match guard.get(key) {
        None => None,
        Some(Entry::Simple(value)) => Some(value.clone()),
        Some(Entry::Expire(value, ex)) =>
            if &Instant::now() > ex { None } else {
                Some(value.clone())
            },
    }
}

pub fn set(db: &DB, key: String, value: String, ex: Option<Duration>) {
    let entry = match ex {
        None => Entry::Simple(value),
        Some(duration) => {
            Entry::Expire(value, Instant::now() + duration)
        }
    };
    db.lock().unwrap().insert(key, entry);
}
