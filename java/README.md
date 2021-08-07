## Microbenchmarking reading parquet file

### Parquet reader

    mvn package 
    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet raw

Elapsed: 831ms

### Spark vectorized Parquet reader (one column only because complex type is not supported)

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet spark

Elapsed: 1163ms