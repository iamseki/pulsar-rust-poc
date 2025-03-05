## Pulsar Rust Client PoC

This PoC aims to assess the complexity and feasibility of implementing a Pulsar consumer using the [pulsar-rs library](https://github.com/streamnative/pulsar-rs) for a use case that requires the following properties:


- **Batch Consumption with Timeout**: The consumer should process messages in batches, either up to N messages or within a specified timeout.  
- **Bounded processing**: Uncontrolled power is useless, we need to limit the number of concurrent tasks.
- **Acepptable Throughput**: Even with limited parallel task processing, we shouldn't have to wait for all tasks to complete before processing new ones. The mechanism used should allow handling new tasks as soon as one finishes.
- **Long-Running Execution**: The consumer should run indefinitely.

> This PoC was not designed with observability, error handling, extensibility, or idiomatic Rust best practices in mind. The focus is solely on the concepts above.

### Setup

To run the PoC:

- Start a Pulsar container with `docker compose up -d`
- Run the producer multiple times: `cargo run --bin producer`
- Run the consumer: `cargo run --bin consumer`

> You can uncomment `tokio-debug-console` to enable debugging with tokio-debug-console: `RUSTFLAGS="--cfg tokio_unstable" cargo run --bin consumer`

The following variables (with default values) control consumer batching and throttling:
```rust
    const BATCH_SIZE: usize = 10000;
    const TIMEOUT_MS: Duration = Duration::from_millis(2000);
    const MAX_CONCURRENT_THREADS: usize = 100;
```

```sh
=== System Info ===:
    OS: "Linux 22.04 Ubuntu"
    Kernel Version: "6.8.0-52-generic"
    Rust Version: rustc 1.85.0 (4d91de4e4 2025-02-17)
    Total Memory: 14 GB
    Total Swap: 1 GB
    CPU Brand: 12
    CPUs: "AMD Ryzen 5 5500U with Radeon Graphics"
    Tokio Executor Threads: 12 
=== System Info ===

- 11.328824615s total, 1.132882ms avg per iteration for tasks_processed 10000
- 11.399093167s total, 1.139909ms avg per iteration for tasks_processed 10000
- 7.70495259s total, 1.54099ms avg per iteration for tasks_processed 5000
```

### Conclusion


