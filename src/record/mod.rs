use crypto::{digest::Digest, sha1::Sha1};

pub type HashedKey = [u8; 20];

pub fn hash_sha1(key: &str) -> HashedKey {
    let mut hasher = Sha1::new();
    let mut hashed_key: HashedKey = [0; 20];

    hasher.input_str(key);
    hasher.result(&mut hashed_key);

    hashed_key
}

pub fn hash_sha1_bytes(key: &[u8]) -> HashedKey {
    let mut hasher = Sha1::new();
    let mut hashed_key: HashedKey = [0; 20];

    hasher.input(key);
    hasher.result(&mut hashed_key);

    hashed_key
}

#[derive(Debug, Clone)]
pub struct Record {
    pub key: String,
    pub value: Vec<u8>,
    pub hash: HashedKey,
    pub timestamp: u64,
}

impl Record {
    pub fn new(key: String, value: Vec<u8>) -> Record {
        let timestamp = crate::time::now();
        Record::new_with_timestamp(key, value, timestamp)
    }

    pub fn new_with_timestamp(key: String, value: Vec<u8>, timestamp: u64) -> Record {
        let hash = hash_sha1(&key);
        Record { key, value, hash, timestamp }
    }

    pub fn size_of(&self) -> usize {
        2 + 4 + 8 + self.key.len() + self.value.len()
    }
}
