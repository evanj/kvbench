use std::fmt::Debug;
use std::sync::Arc;
use std::{
    borrow::{Borrow, Cow},
    collections::{BTreeMap, HashMap},
    error::Error,
    fmt::Display,
    io::Write,
    process::{Child, Command, Stdio},
    sync::{Mutex, MutexGuard},
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
    pub fn new_other<T, StringType: Into<String>>(message: StringType) -> Result<T, Self> {
        Err(Self::Other(message.into()))
    }
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

/// A thread-safe key/value store. To access the store, first create a connection. A connection
/// cannot be reused across threads. The connection then provides the get/put methods itself.
/// The get method on a connection returns a read guard so it can return a reference.
///
/// A KVStore must be thread-safe, so methods are on shared references &self, rather than
/// mutable (exclusive) references.
pub trait KVStore: Sync {
    type Connection: KVStoreConnection;

    /// Creates a new connection to the KVStore, which can be used by a single thread. This can
    /// represent a network connection, or be used for epoch-based reclaimation.
    fn connect(&self) -> Result<Self::Connection, KVError>;
}

/// A single thread's connection to a KV store. This exists to support a connection per thread
/// model, or to implement epoch-based reclaimation, where we need to know all possible users of
/// a store.
///
/// This trait is Send, because it is possible to move it to another thread, but is not Sync,
/// because it can't be shared across threads. The methods take &mut so values can be cached, since
/// the connection cannot be shared between threads.
pub trait KVStoreConnection: Send {
    type ReadGuard<'a>: KVReadGuard<'a>
    where
        Self: 'a;

    /// Stores the key, value pair.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError>;

    /// Returns a [`ReadGuard`] to perform a get.
    fn get_guard(&mut self) -> Result<Self::ReadGuard<'_>, KVError>;
}

pub trait KVReadGuard<'a>: Debug {
    /// Returns `Some(slice)` if the key exists, `None` if the key is not set, or an error.
    /// This requires a mutable reference because this may involve network communcation or storing
    /// a result.
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError>;
}

// KVStoreSingleThread is not thread-safe. Its methods take mutable references &mut self. This is
// true even for get, since there may be caching or other things. This trait exists for
// LockedKVStore.
pub trait KVStoreSingleThreaded: Send {
    // Stores the key, value pair.
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError>;

    // Returns an `Option<&[u8]>` on success, or a KVError.
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError>;
}

pub struct KVStoreSingleThreadedConnection<'a, StoreT: KVStoreSingleThreaded> {
    store: &'a mut StoreT,
}

impl<'a, StoreT: KVStoreSingleThreaded> KVStoreSingleThreadedConnection<'a, StoreT> {
    pub fn new(store: &'a mut StoreT) -> Self {
        Self { store }
    }
}

impl<'a, StoreT: KVStoreSingleThreaded> KVStoreConnection
    for KVStoreSingleThreadedConnection<'a, StoreT>
{
    type ReadGuard<'b> = KVSingleThreadedReadGuard<'b, StoreT> where Self: 'b;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        self.store.put(key, value)
    }

    fn get_guard(&mut self) -> Result<Self::ReadGuard<'_>, KVError> {
        Ok(Self::ReadGuard::new(self.store))
    }
}

pub struct LockedKVStore<T: KVStoreSingleThreaded> {
    store: Arc<Mutex<T>>,
}

impl<T: KVStoreSingleThreaded> LockedKVStore<T> {
    pub fn new(store: T) -> Self {
        Self {
            store: Arc::new(Mutex::new(store)),
        }
    }
}

impl<T: KVStoreSingleThreaded + Send> KVStore for LockedKVStore<T> {
    type Connection = LockedKVStoreConnection<T>;

    fn connect(&self) -> Result<LockedKVStoreConnection<T>, KVError> {
        Ok(LockedKVStoreConnection::new(self.store.clone()))
    }
}

pub struct LockedKVStoreConnection<T: KVStoreSingleThreaded + Send> {
    locked_store: Arc<Mutex<T>>,
}

impl<T: KVStoreSingleThreaded + Send> LockedKVStoreConnection<T> {
    const fn new(locked_store: Arc<Mutex<T>>) -> Self {
        Self { locked_store }
    }
}

impl<T: KVStoreSingleThreaded + Send> KVStoreConnection for LockedKVStoreConnection<T> {
    type ReadGuard<'a> = LockedKVReadGuard<'a, T>  where T: 'a;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        let mut guard = self.locked_store.lock().unwrap();
        guard.put(key, value)
    }

    fn get_guard(&mut self) -> Result<Self::ReadGuard<'_>, KVError> {
        let guard = self.locked_store.lock().unwrap();
        Ok(LockedKVReadGuard::new(guard))
    }
}

#[allow(dead_code)]
pub struct LockedKVReadGuard<'a, T: KVStoreSingleThreaded + Send> {
    guard: MutexGuard<'a, T>,
}

impl<'a, T: KVStoreSingleThreaded + Send> Debug for LockedKVReadGuard<'a, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TODO")
    }
}

#[allow(dead_code)]
impl<'a, T: KVStoreSingleThreaded + Send> LockedKVReadGuard<'a, T> {
    fn new(guard: MutexGuard<'a, T>) -> Self {
        Self { guard }
    }
}

impl<'a, T: KVStoreSingleThreaded + Send> KVReadGuard<'a> for LockedKVReadGuard<'a, T> {
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError> {
        self.guard.get(key)
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

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError> {
        if let Some(value) = self.store.get(key) {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

impl KVStoreConnection for HashMapStore {
    type ReadGuard<'a> = KVSingleThreadedReadGuard<'a, Self>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        KVStoreSingleThreaded::put(self, key, value)
    }

    fn get_guard(&mut self) -> Result<Self::ReadGuard<'_>, KVError> {
        Ok(Self::ReadGuard::new(self))
    }
}

pub struct KVSingleThreadedReadGuard<'a, StoreT: KVStoreSingleThreaded> {
    store: &'a mut StoreT,
}

impl<StoreT: KVStoreSingleThreaded> Debug for KVSingleThreadedReadGuard<'_, StoreT> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KVSingleThreadedReadGuard TODO")
    }
}

impl<'a, StoreT: KVStoreSingleThreaded> KVSingleThreadedReadGuard<'a, StoreT> {
    fn new(store: &'a mut StoreT) -> Self {
        Self { store }
    }
}

impl<'a, StoreT: KVStoreSingleThreaded> KVReadGuard<'a> for KVSingleThreadedReadGuard<'a, StoreT> {
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError> {
        self.store.get(key)
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

impl Default for BTreeMapStore {
    fn default() -> Self {
        Self::new()
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

    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError> {
        if let Some(value) = self.store.get(key) {
            Ok(Some(value))
        } else {
            Ok(None)
        }
    }
}

pub struct RedisStore {
    client: redis::Client,

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
        Ok(Self {
            client,
            redis_process,
        })
    }
}

impl KVStore for RedisStore {
    type Connection = RedisConnection;

    fn connect(&self) -> Result<RedisConnection, KVError> {
        let redis_connection = self.client.get_connection()?;
        let connection = RedisConnection::new(redis_connection);
        Ok(connection)
    }
}

pub struct RedisConnection {
    connection: redis::Connection,
}

impl RedisConnection {
    const fn new(connection: redis::Connection) -> Self {
        Self { connection }
    }
}

impl KVStoreConnection for RedisConnection {
    type ReadGuard<'a> = RedisReadGuard<'a>;

    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), KVError> {
        self.connection.set(key, value)?;
        Ok(())
    }

    fn get_guard(&mut self) -> Result<Self::ReadGuard<'_>, KVError> {
        Ok(RedisReadGuard::new(self))
    }
}

#[derive(Debug)]
pub struct RedisReadGuard<'a> {
    connection: &'a mut RedisConnection,
    result: Vec<u8>,
}

impl Debug for RedisConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RedisConnection{{ TODO STUFF }}")
    }
}

impl<'a> RedisReadGuard<'a> {
    fn new(connection: &'a mut RedisConnection) -> Self {
        Self {
            connection,
            result: Vec::new(),
        }
    }
}

impl<'a> KVReadGuard<'a> for RedisReadGuard<'a> {
    fn get(&mut self, key: &[u8]) -> Result<Option<&[u8]>, KVError> {
        let result: Option<Vec<u8>> = self.connection.connection.get(key)?;
        match result {
            None => Ok(None),

            // move the redis result vec into the read guard
            // TODO: Avoid an allocation on each get?
            Some(result_bytes) => {
                self.result = result_bytes;
                Ok(Some(&self.result))
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
    use std::thread::Scope;

    #[test]
    fn test_hash_store() {
        let mut store = HashMapStore::new();
        test_kv_single_threaded(&mut store).expect("test failed");

        let locked_map = LockedKVStore::new(store);
        test_kv_thread_safe(&locked_map).unwrap();
    }

    #[test]
    fn test_btree_store() {
        let mut store = BTreeMapStore::new();
        test_kv_single_threaded(&mut store).expect("test failed");

        let locked_btree = LockedKVStore::new(store);
        test_kv_thread_safe(&locked_btree).unwrap();
    }

    #[test]
    fn test_redis_store() {
        let redis = RedisSpawner::new().unwrap();
        let store = RedisStore::new(&redis.localhost_url()).expect("connect must succeed");
        test_kv_thread_safe(&store).expect("test failed");
    }

    fn test_kv_single_threaded<T: KVStoreSingleThreaded>(store: &mut T) -> Result<(), KVError> {
        let empty_bytes = b"";
        let foo_bytes = b"foo";

        // test get/set not exists / empty bytes / over write
        assert_eq!(store.get(empty_bytes)?, None);
        store.put(empty_bytes, empty_bytes)?;
        assert_eq!(store.get(empty_bytes)?.unwrap(), empty_bytes);
        store.put(empty_bytes, foo_bytes)?;
        assert_eq!(store.get(empty_bytes)?.unwrap(), foo_bytes);

        // test another key
        assert_eq!(store.get(foo_bytes)?, None);
        store.put(foo_bytes, empty_bytes)?;
        assert_eq!(store.get(foo_bytes)?.unwrap(), empty_bytes);
        assert_eq!(store.get(empty_bytes)?.unwrap(), foo_bytes);

        Ok(())
    }

    /// Asserts that two byte slices are equal and prints a readable message if they are not
    fn assert_eq_bytes(first: &[u8], second: &[u8]) {
        // TODO: use escape_ascii
        assert!(
            first == second,
            "first={} != second={}",
            str::from_utf8(first).unwrap(),
            str::from_utf8(second).unwrap()
        );
    }

    fn test_kv_thread_safe<'a, T: KVStore>(store: &'a T) -> Result<(), KVError> {
        // test concurrent reads/writes
        let task_a = b"task_a";
        let task_b = b"task_b";
        let shared_key = b"shared_key";

        // spawn two threads to write to their own key, and to a shared key
        thread::scope(|s: &Scope<'_, 'a>| -> Result<(), KVError> {
            let thread_a = s.spawn(|| -> Result<(), KVError> {
                let mut connection = store.connect()?;
                write_two_keys(&mut connection, task_a, shared_key, task_a)
            });
            let thread_b = s.spawn(|| -> Result<(), KVError> {
                let mut connection = store.connect()?;
                write_two_keys(&mut connection, task_b, shared_key, task_b)
            });

            thread_a.join().unwrap()?;
            thread_b.join().unwrap()?;
            Ok(())
        })?;

        // read the keys!
        let mut kv_connection = store.connect()?;
        {
            let mut guard = kv_connection.get_guard()?;
            let result = guard.get(task_a)?.unwrap();
            assert_eq_bytes(task_a, result);
        }

        {
            let mut guard = kv_connection.get_guard()?;
            let result: &[u8] = guard.get(task_b)?.unwrap();
            assert_eq_bytes(task_b, result);
        }

        // on my machine this seems to be more likely to be task_b that wins the race, but it is
        // not guaranteed.
        {
            let mut guard = kv_connection.get_guard()?;
            let result = guard.get(shared_key)?.unwrap();
            assert!(result == task_a || result == task_b);
        }

        Ok(())
    }

    /// Writes value to both `key_one` and `key_two`.
    fn write_two_keys<T: KVStoreConnection>(
        connection: &mut T,
        key_one: &[u8],
        key_two: &[u8],
        value: &[u8],
    ) -> Result<(), KVError> {
        {
            let mut guard = connection.get_guard()?;
            let result = guard.get(key_one)?;
            assert_eq!(None, result);
        }

        connection.put(key_one, value)?;
        connection.put(key_two, value)?;

        let mut guard = connection.get_guard()?;
        let output = guard.get(key_one)?.unwrap();
        assert_eq!(output, value);

        Ok(())
    }

    #[test]
    fn test_kverror_other_types() {
        // ensure new_other can take a String or a &str
        let e1 = KVError::new_other::<(), _>("hello").unwrap_err();
        let e2 = KVError::new_other::<(), _>("hello".to_string()).unwrap_err();
        assert_eq!(e1, e2);
    }
}
