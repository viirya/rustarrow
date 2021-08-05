## Microbenchmarking reading parquet file

### Parquet reader

    mvn package 
    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet

Elapsed: 2267ms
