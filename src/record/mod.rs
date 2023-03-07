use std::time::{SystemTime, UNIX_EPOCH};

use crypto::{digest::Digest, sha1::Sha1};

pub type HashedKey = [u8; 20];

pub fn hash_sha1(key: &str) -> HashedKey {
    let mut hasher = Sha1::new();
    let mut hashed_key: HashedKey = [0; 20];

    hasher.input_str(key);
    hasher.result(&mut hashed_key);

    hashed_key
}

#[derive(Debug, Clone)]
pub struct Record {
    pub key: String,
    pub value: String,
    pub hash: HashedKey,
    pub timestamp: u64,
}

impl Record {
    pub fn new(key: String, value: String) -> Record {
        let hash = hash_sha1(&key);
        let timestamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        Record { key, value, hash, timestamp }
    }

    pub fn size_of(&self) -> usize {
        return 2 + 4 + 8 + self.key.len() + self.value.len() 
    }
}
