use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::Display,
    io::Write,
    process::{Child, Command, Stdio},
    thread::sleep,
    time::Duration,
};

use nix::{
    sys::signal::{self, Signal},
    unistd::Pid,
};
use redis::{Commands, RedisError};

#[derive(Debug, PartialEq)]
pub enum KVError {
    KeyNotFound,
    RedisError(RedisError),
    Other(String),
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

            Self::Other(msg) => f.write_str(msg),
        }
    }
}

impl From<RedisError> for KVError {
    fn from(err: RedisError) -> Self {
        Self::RedisError(err)
    }
}

pub trait KVStore {
    // Stores the key, value pair.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError>;
    // Returns the value for key, or a KVError::KeyNotFound if the key is not set.
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
    connection: redis::Connection,
    // stores the get result since KVStore::get returns a &[u8]
    get_result: Vec<u8>,

    // incorrect warning: this is used by the Drop trait
    // this must be last in the struct: fields are dropped in source code order and we want the
    // connection to be closed before we shut down redis
    #[allow(dead_code)]
    redis_process: Option<RedisSpawner>,
}

impl RedisStore {
    pub fn new(redis_url: &str) -> Result<Self, KVError> {
        // TODO: rewrite using mutable vars? probably easier to understand
        let (url, redis_process) = if redis_url.is_empty() {
            println!("redis_url unset; starting localhost redis ...");
            let spawner = RedisSpawner::new()
                .map_err(|dyn_err| KVError::Other(format!("error spawning redis: {dyn_err}")))?;
            (Cow::from(spawner.localhost_url()), Some(spawner))
        } else {
            (Cow::from(redis_url), None)
        };

        let client = redis::Client::open(url.borrow())?;
        let connection = client.get_connection()?;
        Ok(Self {
            connection,
            get_result: Vec::new(),
            redis_process,
        })
    }
}

impl KVStore for RedisStore {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        self.connection.set(key, value)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<&[u8], KVError> {
        let result: Option<Vec<u8>> = self.connection.get(key)?;
        match result {
            None => Err(KVError::KeyNotFound),
            Some(result_bytes) => {
                self.get_result = result_bytes;
                Ok(&self.get_result)
            }
        }
    }
}

struct RedisSpawner {
    child: Child,
}

// TODO: Pick dynamically
const REDIS_PORT: u16 = 12346;

const REDIS_CONFIG: &str = r#"
# Docs: https://redis.io/docs/management/config/
# localhost only
bind 127.0.0.1 ::1
port 12346

# default is notice; debug has too much
loglevel verbose

# disable snapshotting: in memory only
save ""

# TODO: experiment with these settings
# io-threads 4
# io-threads-do-reads no
"#;

impl RedisSpawner {
    fn new() -> Result<Self, Box<dyn Error>> {
        let mut child = Command::new("redis-server")
            .arg("-")
            .stdin(Stdio::piped())
            .spawn()?;

        // write the config
        let mut stdin = child.stdin.take().ok_or("BUG: stdin must be pipe")?;
        stdin.write_all(REDIS_CONFIG.as_bytes())?;
        drop(stdin);

        // TODO: wait until server is listening
        sleep(Duration::from_millis(100));

        Ok(Self { child })
    }

    // Overriding unused_self because this should eventually use a randomly selected port
    #[allow(clippy::unused_self)]
    fn localhost_url(&self) -> String {
        format!("redis://localhost:{REDIS_PORT}/")
    }
}

impl Drop for RedisSpawner {
    fn drop(&mut self) {
        // send SIGTERM to Redis and wait for exit
        signal::kill(
            Pid::from_raw(self.child.id().try_into().unwrap()),
            Signal::SIGTERM,
        )
        .expect("failed to send SIGTERM to Redis child");
        self.child.wait().expect("failed waiting for Redis to exit");
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_hash_store() {
        let mut store = HashMapStore::new();
        test_generic_store(&mut store).expect("test failed");
    }

    #[test]
    fn test_btree_store() {
        let mut store = HashMapStore::new();
        test_generic_store(&mut store).expect("test failed");
    }

    #[test]
    fn test_redis_store() {
        let redis = RedisSpawner::new().unwrap();
        let mut store = RedisStore::new(&redis.localhost_url()).expect("connect must succeed");
        test_generic_store(&mut store).expect("test failed");
    }

    fn test_generic_store<T: KVStore>(store: &mut T) -> Result<(), KVError> {
        let empty_bytes = b"";
        let foo_bytes = b"foo";

        // test get/set not exists / empty bytes / over write
        assert_eq!(store.get(empty_bytes), Err(KVError::KeyNotFound));
        store.put(empty_bytes, empty_bytes)?;
        assert_eq!(store.get(empty_bytes)?, empty_bytes);
        store.put(empty_bytes, foo_bytes)?;
        assert_eq!(store.get(empty_bytes)?, foo_bytes);

        // test another key
        assert_eq!(store.get(foo_bytes), Err(KVError::KeyNotFound));
        store.put(foo_bytes, empty_bytes)?;
        assert_eq!(store.get(foo_bytes)?, empty_bytes);
        assert_eq!(store.get(empty_bytes)?, foo_bytes);

        Ok(())
    }
}
