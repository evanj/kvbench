use rand::prelude::Distribution;
use rand::SeedableRng;
use std::time::{Duration, Instant};

use kvbench::*;

/// Configuration for the key/value benchmark.
#[derive(argh::FromArgs)]
struct BenchmarkConfig {
    /// number of keys.
    #[argh(option, default = "10")]
    num_keys: usize,

    /// measurement duration.
    #[argh(
        option,
        default = "Duration::from_secs(10)",
        from_str_fn(argh_parse_go_duration)
    )]
    measure_duration: Duration,

    /// kind of store (BTreeMap, HashMap)
    #[argh(option, default = "StoreKind::HashMap")]
    store_kind: StoreKind,

    /// URL to connect to redis e.g. redis:///localhost:12345
    #[argh(option, default = "String::new()")]
    redis_url: String,
}

#[derive(strum::EnumString)]
enum StoreKind {
    HashMap,
    BTreeMap,
    Redis,
}

impl StoreKind {
    fn create(&self, config: &BenchmarkConfig) -> Result<Box<dyn KVStore>, KVError> {
        match self {
            Self::HashMap => Ok(Box::new(HashMapStore::new())),
            Self::BTreeMap => Ok(Box::new(BTreeMapStore::new())),
            Self::Redis => Ok(Box::new(RedisStore::new(&config.redis_url)?)),
        }
    }
}

/// Parses a duration using Go's formats, with the signature required by argh.
fn argh_parse_go_duration(s: &str) -> Result<Duration, String> {
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
    rng: rand_xoshiro::Xoshiro256Plus,
    // num_keys: usize,
    key_buffer: [u8; 8],
    key_range: rand::distributions::Uniform<u64>,
}

impl KeyGenerator {
    fn new(num_keys: usize) -> Self {
        // the rand book suggests Xoshiro256Plus is fast and pretty good:
        // https://rust-random.github.io/book/guide-rngs.html
        let rng = rand_xoshiro::Xoshiro256Plus::from_entropy();
        Self {
            rng,
            // num_keys,
            key_buffer: [0u8; 8],
            key_range: rand::distributions::Uniform::from(0..num_keys as u64),
        }
    }

    fn next_key(&mut self) -> &[u8] {
        let key = self.key_range.sample(&mut self.rng) * 2;
        self.key_buffer = key.to_be_bytes();
        &self.key_buffer[..]
    }
}

fn fill_store(store: &mut dyn KVStore, num_keys: usize) -> Result<(), KVError> {
    println!("filling store with {num_keys} keys ...");

    let mut key_buffer: [u8; 8];
    let start = Instant::now();
    for i in 0..num_keys {
        let k = i as u64 * 2;
        key_buffer = k.to_be_bytes();
        // println!("put i={i} k={k} bytes={:x?}", &key_buffer[..]);
        let key_slice = &key_buffer[..];
        store.put(key_slice, key_slice)?;
    }
    let end = Instant::now();
    let duration = end - start;
    let data_bytes = 16 * num_keys;
    println!(
        "filled in {duration:?} ; {:.1} keys/sec; {:.1} MiB of data",
        num_keys as f64 / duration.as_secs_f64(),
        data_bytes as f64 / 1024.0 / 1024.0,
    );

    Ok(())
}

fn run_bench(
    store: &mut dyn KVStore,
    key_gen: &mut KeyGenerator,
    measure_duration: Duration,
) -> Result<(), KVError> {
    let mut requests = 0usize;
    let start = Instant::now();
    let measure_end = start + measure_duration;

    let mut value_bytes_array: [u8; 8];
    loop {
        let now = Instant::now();
        if now >= measure_end {
            break;
        }

        requests += 1;
        value_bytes_array = requests.to_le_bytes();
        let key_slice = key_gen.next_key();
        store.put(key_slice, &value_bytes_array[..])?;
    }

    let end = Instant::now();
    let duration = end - start;
    println!(
        "{} requests in {duration:?}; {:.3} requests/sec",
        requests,
        requests as f64 / duration.as_secs_f64()
    );

    Ok(())
}

fn main() -> Result<(), KVError> {
    let config: BenchmarkConfig = argh::from_env();
    println!(
        "running benchmark num_keys={} measure_duration={:?}",
        config.num_keys, config.measure_duration
    );

    let mut store = config.store_kind.create(&config)?;
    fill_store(store.as_mut(), config.num_keys)?;

    let mut key_gen = KeyGenerator::new(config.num_keys);
    run_bench(store.as_mut(), &mut key_gen, config.measure_duration)?;
    Ok(())
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
