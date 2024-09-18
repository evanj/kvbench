all:
	cargo fmt
	RUST_BACKTRACE=1 cargo test
	cargo check
	# disallow warnings so they fail CI
	cargo clippy --all-targets -- -D warnings

run_bench:
	RUSTFLAGS="-C target-cpu=native" cargo build --profile=release-nativecpu
	bash runbasic.sh

run_perf:
	RUSTFLAGS="-C target-cpu=native" cargo build --profile=release-nativecpu
	perf record --call-graph=dwarf target/release-nativecpu/kvbench --num-keys 1000000

allow_perf:
	echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
