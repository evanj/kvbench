all:
	cargo fmt
	cargo test
	cargo check
	# https://zhauniarovich.com/post/2021/2021-09-pedantic-clippy/#paranoid-clippy
	# -D clippy::restriction is way too "safe"/careful
	# -D clippy::pedantic is also probably too safe
	# nursery: -A clippy::option-if-let-else: I don't find using map_or clearer than if/else
	# https://rust-lang.github.io/rust-clippy/master/index.html#option_if_let_else
	cargo clippy --all-targets --all-features -- \
		-D warnings \
		-D clippy::nursery \
		-A clippy::option-if-let-else \
		-D clippy::pedantic \
		-A clippy::cast_precision_loss

run_bench:
	RUSTFLAGS="-C target-cpu=native" cargo build --profile=release-nativecpu
	bash runbasic.sh

run_perf:
	RUSTFLAGS="-C target-cpu=native" cargo build --profile=release-nativecpu
	perf record --call-graph=dwarf target/release-nativecpu/kvbench --num-keys 1000000

allow_perf:
	echo -1 | sudo tee /proc/sys/kernel/perf_event_paranoid
