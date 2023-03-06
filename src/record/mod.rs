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
}

impl Record {
    pub fn new(key: String, value: String) -> Record {
        let hash = hash_sha1(&key);
        Record { key, value, hash }
    }
}
