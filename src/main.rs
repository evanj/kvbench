use clap::Parser;
use kvbench::{
    BTreeMapStore, HashMapStore, KVError, KVStore, KVStoreConnection, KVStoreSingleThreaded,
    KVStoreSingleThreadedConnection, LockedKVStore, RedisStore, SkipListStore,
};
use rand::prelude::Distribution;
use rand::SeedableRng;
use std::{
    sync::{
        atomic::{self, AtomicBool},
        Arc,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};

/// Configuration for the key/value benchmark.
#[derive(clap::Parser)]
#[command(version, about)]
struct BenchmarkConfig {
    /// number of keys.
    #[arg(long, default_value_t = 10)]
    num_keys: usize,

    /// measurement duration.
    #[arg(
        long, default_value = "10s",
        value_parser=parse_go_duration,
    )]
    measure_duration: Duration,

    /// kind of store (`STHashMap`, `STBTreeMap`, `LockedHashMap`, `LockedBTreeMap`, `Redis`)
    #[arg(long, default_value_t = StoreKind::STHashMap)]
    store_kind: StoreKind,

    /// URL to connect to redis e.g. <redis:///localhost:12345>
    #[arg(long, default_value = "")]
    redis_url: String,

    /// workers threads for both filling and benchmarking
    // #[argh(option, default = "1")]
    #[arg(long, default_value_t = 1)]
    num_threads: usize,
}

#[derive(strum::EnumString, strum::Display, Clone, PartialEq)]
enum StoreKind {
    STHashMap,
    STBTreeMap,
    LockedHashMap,
    LockedBTreeMap,
    Redis,
    SkipList,
}

/// Parses a duration using Go's formats, with the signature required by argh.
fn parse_go_duration(s: &str) -> Result<Duration, String> {
    let result = go_parse_duration::parse_duration(s);
    match result {
        Err(err) => Err(format!("{err:?}")),
        Ok(nanos) => {
            assert!(nanos >= 0);
            Ok(Duration::from_nanos(
                nanos.try_into().expect("BUG: duration must be >= 0"),
            ))
        }
    }
}

struct KeyGenerator {
    rng: rand::rngs::SmallRng,
    // num_keys: usize,
    key_buffer: [u8; 8],
    key_range: rand::distributions::Uniform<u64>,
}

impl KeyGenerator {
    fn new(num_keys: usize) -> Self {
        let rng = rand::rngs::SmallRng::from_entropy();
        Self {
            rng,
            // num_keys,
            key_buffer: [0u8; 8],
            key_range: rand::distributions::Uniform::from(0..num_keys as u64),
        }
    }

    /// Generates a random key that is should exist.
    fn random_key_exists(&mut self) -> &[u8] {
        let key = self.key_range.sample(&mut self.rng) * 2;
        self.key_buffer = key.to_be_bytes();
        &self.key_buffer[..]
    }
}

fn run_bench_single_threaded<StoreT: KVStoreSingleThreaded + 'static>(
    mut store: StoreT,
    config: &BenchmarkConfig,
) -> Result<(), KVError> {
    if config.num_threads != 1 {
        return KVError::new_other(format!(
            "type {} is single threaded; num_threads must be 1 (is {})",
            config.store_kind, config.num_threads
        ));
    }

    let mut connection = KVStoreSingleThreadedConnection::new(&mut store);
    fill_store(&mut connection, config.num_keys)?;

    let key_gen = KeyGenerator::new(config.num_keys);

    let stop_test = Arc::new(AtomicBool::new(false));
    let thread_stop_test = stop_test.clone();

    let thread_handles = vec![thread::spawn(move || {
        let connection = KVStoreSingleThreadedConnection::new(&mut store);
        simulate_work_single_thread(connection, key_gen, &thread_stop_test)
    })];

    wait_for_bench(thread_handles, &stop_test, config.measure_duration);
    Ok(())
}

fn main() -> Result<(), KVError> {
    let config = BenchmarkConfig::parse();
    println!(
        "running benchmark store_kind={} num_keys={} measure_duration={:?}",
        config.store_kind, config.num_keys, config.measure_duration
    );

    match config.store_kind {
        StoreKind::STBTreeMap => {
            let store = BTreeMapStore::new();
            run_bench_single_threaded(store, &config)
        }
        StoreKind::STHashMap => {
            let store = HashMapStore::new();
            run_bench_single_threaded(store, &config)
        }
        StoreKind::LockedBTreeMap => {
            let store = LockedKVStore::new(BTreeMapStore::new());
            run_bench_multi_threaded(&store, &config)
        }
        StoreKind::LockedHashMap => {
            let store = LockedKVStore::new(HashMapStore::new());
            run_bench_multi_threaded(&store, &config)
        }
        StoreKind::Redis => {
            let store = RedisStore::new(&config.redis_url)?;
            run_bench_multi_threaded(&store, &config)
        }
        StoreKind::SkipList => {
            let store = SkipListStore::new();
            run_bench_multi_threaded(&store, &config)
        }
    }
}

fn fill_store<T: KVStoreConnection>(connection: &mut T, num_keys: usize) -> Result<(), KVError> {
    println!("filling with {num_keys} keys using 1 thread ...");

    // TODO: fill on multiple threads
    let mut key_buffer: [u8; 8];
    let start = Instant::now();
    for i in 0..num_keys {
        let k = i as u64 * 2;
        key_buffer = k.to_be_bytes();
        // println!("put i={i} k={k} bytes={:x?}", &key_buffer[..]);
        let key_slice = &key_buffer[..];
        connection.put(key_slice, key_slice)?;
    }
    let end = Instant::now();
    let duration = end - start;
    let data_bytes = 16 * num_keys;
    println!(
        "filled with 1 thread in {duration:?} ; {:.1} keys/sec; {:.1} MiB of data",
        num_keys as f64 / duration.as_secs_f64(),
        data_bytes as f64 / 1024.0 / 1024.0,
    );

    Ok(())
}

fn run_bench_multi_threaded<T: KVStore + 'static>(
    store: &T,
    config: &BenchmarkConfig,
) -> Result<(), KVError> {
    let mut connection = store.connect()?;
    fill_store(&mut connection, config.num_keys)?;

    let mut thread_handles = Vec::with_capacity(config.num_threads);
    let stop_test = Arc::new(AtomicBool::new(false));

    for _ in 0..config.num_threads {
        let thread_connection: T::Connection = store.connect()?;
        let thread_stop_test = stop_test.clone();
        let thread_key_gen = KeyGenerator::new(config.num_keys);
        let thread_handle = std::thread::spawn(move || {
            simulate_work_single_thread(thread_connection, thread_key_gen, &thread_stop_test)
        });
        thread_handles.push(thread_handle);
    }

    wait_for_bench(thread_handles, &stop_test, config.measure_duration);

    Ok(())
}

fn wait_for_bench(
    thread_handles: Vec<JoinHandle<usize>>,
    stop_test: &Arc<AtomicBool>,
    measure_duration: Duration,
) {
    // TODO: Add a barrier so test threads start at closer to the same time
    assert!(!stop_test.load(atomic::Ordering::SeqCst));
    let start = Instant::now();
    thread::sleep(measure_duration);
    stop_test.store(true, atomic::Ordering::SeqCst);
    let duration = start.elapsed();

    let mut total_requests = 0;
    for handle in thread_handles {
        let requests = handle.join().unwrap();
        total_requests += requests;
    }

    println!(
        "{total_requests} requests in {duration:?}; {:.3} requests/sec",
        total_requests as f64 / duration.as_secs_f64()
    );
}

/// Simulates a single client in a thread using a connection.
fn simulate_work_single_thread<T: KVStoreConnection>(
    mut connection: T,
    mut key_gen: KeyGenerator,
    stop_test: &Arc<AtomicBool>,
) -> usize {
    let mut requests = 0usize;
    let mut value_bytes_array: [u8; 8];
    while !stop_test.load(atomic::Ordering::SeqCst) {
        requests += 1;
        value_bytes_array = requests.to_le_bytes();
        let key_slice = key_gen.random_key_exists();
        connection.put(key_slice, &value_bytes_array[..]).unwrap();
    }

    requests
}
