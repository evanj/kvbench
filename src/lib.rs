use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::Display,
    io::Write,
    process::{Child, Command, Stdio},
    sync::Mutex,
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

/// A thread-safe key/value store. All methods are on shared references &self, rather than
/// mutable (exclusive) references.
pub trait KVStore: Sync {
    /// Stores the key, value pair.
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KVError>;

    /// Returns the value for key, or a KVError::KeyNotFound if the key is not set.
    /// TODO: Return a ReadGuard for a zero-copy get.
    fn get(&self, key: &[u8], output: &mut Vec<u8>) -> Result<(), KVError>;
}

// KVStoreSingleThread is not thread-safe. Its methods take mutable references &mut self. This is
// true even for get, since there may be caching or other things. This trait exists for
// LockedKVStore.
pub trait KVStoreSingleThreaded {
    // Stores the key, value pair.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError>;
    // Returns the value for key, or a KVError::KeyNotFound if the key is not set.
    fn get(&mut self, key: &[u8]) -> Result<&[u8], KVError>;
}

pub struct LockedKVStore<T: KVStoreSingleThreaded + Send> {
    store: Mutex<T>,
}

impl<T: KVStoreSingleThreaded + Send> LockedKVStore<T> {
    fn new(store: T) -> Self {
        Self {
            store: Mutex::new(store),
        }
    }
}

impl<T: KVStoreSingleThreaded + Send> KVStore for LockedKVStore<T> {
    fn put(&self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        let mut guard = self.store.lock().unwrap();
        guard.put(key, value)
    }

    fn get(&self, key: &[u8], output: &mut Vec<u8>) -> Result<(), KVError> {
        let mut guard = self.store.lock().unwrap();
        match guard.get(key) {
            Err(err) => Err(err),
            Ok(bytes) => {
                output.clear();
                output.extend_from_slice(bytes);
                Ok(())
            }
        }
    }
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

impl KVStoreSingleThreaded for HashMapStore {
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

impl KVStoreSingleThreaded for BTreeMapStore {
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

impl KVStoreSingleThreaded for RedisStore {
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
    use std::str;
    use std::thread;

    #[test]
    fn test_hash_store() {
        let mut store = HashMapStore::new();
        test_kv_single_threaded(&mut store).expect("test failed");
    }

    #[test]
    fn test_btree_store() {
        let mut store = BTreeMapStore::new();
        test_kv_single_threaded(&mut store).expect("test failed");

        let locked_btree = LockedKVStore::new(store);
        test_kv_threads(&locked_btree);
    }

    #[test]
    fn test_redis_store() {
        let redis = RedisSpawner::new().unwrap();
        let mut store = RedisStore::new(&redis.localhost_url()).expect("connect must succeed");
        test_kv_single_threaded(&mut store).expect("test failed");
    }

    fn test_kv_single_threaded<T: KVStoreSingleThreaded>(store: &mut T) -> Result<(), KVError> {
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

    fn test_kv_threads<T: KVStore>(store: &T) -> Result<(), KVError> {
        // test concurrent reads/writes
        let task_a = b"task_a";
        let task_b = b"task_b";
        let shared_key = b"shared_key";

        // spawn two threads to write to their own key, and to a shared key
        thread::scope(|s| -> Result<(), KVError> {
            let thread_a = s.spawn(|| write_two_keys(store, task_a, shared_key, task_a));
            let thread_b = s.spawn(|| write_two_keys(store, task_b, shared_key, task_b));

            thread_a.join().unwrap()?;
            thread_b.join().unwrap()?;
            Ok(())
        })?;

        // read the keys!
        let mut output = Vec::new();
        store.get(task_a, &mut output).unwrap();
        assert_eq!(&task_a[..], output);
        store.get(task_b, &mut output).unwrap();
        assert_eq!(
            str::from_utf8(&task_b[..]).unwrap(),
            str::from_utf8(&output).unwrap()
        );

        // on my machine this seems to be more likely to be task_b that wins the race, but it is
        // not guaranteed.
        store.get(shared_key, &mut output).unwrap();
        assert!(output == task_a || output == task_b);

        Ok(())
    }

    /// Writes value to both key_one and key_two.
    fn write_two_keys<T: KVStore>(
        store: &T,
        key_one: &[u8],
        key_two: &[u8],
        value: &[u8],
    ) -> Result<(), KVError> {
        let mut output = Vec::new();
        assert_eq!(
            store.get(key_one, &mut output).unwrap_err(),
            KVError::KeyNotFound
        );

        store.put(key_one, value)?;
        store.put(key_two, value)?;

        store.get(key_one, &mut output).unwrap();
        assert_eq!(output, value);

        Ok(())
    }
}
