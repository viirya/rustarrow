## Microbenchmarking reading parquet file

### Parquet reader

    cargo run data/simple_data.parquet raw

Elapsed: 1.81s

### Arrow reader

    cargo run data/simple_data.parquet arrow

Elapsed: 549.59ms