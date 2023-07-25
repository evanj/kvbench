use std::{
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::Display,
};

use redis::RedisError;

#[derive(Debug)]
pub enum KVError {
    KeyNotFound,
    // Message(String),
    RedisError(RedisError),
}

impl KVError {
    // fn msg<T>(m: &str) -> Result<T, Self> {
    //     let err = Self::Message(String::from(m));
    //     Err(err)
    // }
}

impl Error for KVError {}

impl Display for KVError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeyNotFound => {
                write!(f, "key not found")
            }

            Self::RedisError(err) => {
                write!(f, "redis error: {err}")
            }
        }
    }
}

impl From<RedisError> for KVError {
    fn from(_value: RedisError) -> Self {
        todo!()
    }
}

pub trait KVStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError>;
    fn get(&mut self, key: &[u8]) -> Result<&[u8], KVError>;
}

pub struct HashMapStore {
    store: HashMap<Vec<u8>, Vec<u8>>,
}

impl HashMapStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            store: HashMap::new(),
        }
    }
}

impl Default for HashMapStore {
    fn default() -> Self {
        Self::new()
    }
}

impl KVStore for HashMapStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        // using get_mut was about 10% faster than just insert on a full overwrite workload
        if let Some(value_mut) = self.store.get_mut(key) {
            // about 10% better than *value_mut = Vec::from(value)
            value_mut.truncate(0);
            value_mut.extend_from_slice(value);
        } else {
            let key_vec = Vec::from(key);
            let value_vec = Vec::from(value);
            self.store.insert(key_vec, value_vec);
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<&[u8], KVError> {
        if let Some(value) = self.store.get(key) {
            Ok(value)
        } else {
            Err(KVError::KeyNotFound)
        }
    }
}

pub struct BTreeMapStore {
    store: BTreeMap<Vec<u8>, Vec<u8>>,
}

impl BTreeMapStore {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            store: BTreeMap::new(),
        }
    }
}

impl KVStore for BTreeMapStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        // using get_mut was about 10% faster than just insert on a full overwrite workload
        if let Some(value_mut) = self.store.get_mut(key) {
            // about 10% better than *value_mut = Vec::from(value)
            value_mut.truncate(0);
            value_mut.extend_from_slice(value);
        } else {
            let key_vec = Vec::from(key);
            let value_vec = Vec::from(value);
            self.store.insert(key_vec, value_vec);
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<&[u8], KVError> {
        if let Some(value) = self.store.get(key) {
            Ok(value)
        } else {
            Err(KVError::KeyNotFound)
        }
    }
}

pub struct RedisStore {
    // TODO
    client: redis::Client,
    connection: redis::Connection,
}

impl RedisStore {
    pub fn new(redis_url: &str) -> Result<Self, KVError> {
        let client = redis::Client::open(redis_url)?;
        let connection = client.get_connection()?;
        Ok(Self { client, connection })
    }
}

impl KVStore for RedisStore {
    fn put(&mut self, _key: &[u8], _value: &[u8]) -> Result<(), KVError> {
        todo!()
    }

    fn get(&mut self, _key: &[u8]) -> Result<&[u8], KVError> {
        todo!()
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hash_store() {
        let mut store = HashMapStore::new();
        store.put(b"abc", b"xyz").unwrap();

        let borrowed_get_value = store.get(b"abc").unwrap();
        assert_eq!(borrowed_get_value, b"xyz");

        store.put(b"abc", b"123").unwrap();
        //assert_eq!(borrowed_get_value, b"xyz");
        assert_eq!(b"123", store.get(b"abc").unwrap());
    }

    #[test]
    fn test_btree_store() {
        let mut store = HashMapStore::new();
        store.put(b"abc", b"xyz").unwrap();

        let borrowed_get_value = store.get(b"abc").unwrap();
        assert_eq!(borrowed_get_value, b"xyz");

        store.put(b"abc", b"123").unwrap();
        //assert_eq!(borrowed_get_value, b"xyz");
        assert_eq!(b"123", store.get(b"abc").unwrap());
    }
}
