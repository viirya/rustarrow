## Microbenchmarking reading parquet file

### Parquet reader

    cargo run data/simple_data.parquet raw

Elapsed: 1.05s

### Arrow reader

    cargo run data/simple_data.parquet arrow

Elapsed: 298.88ms