## Microbenchmarking reading parquet file

### Parquet reader

    mvn package 
    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet raw

Elapsed: 831ms

### Spark vectorized Parquet reader (one column only because complex type is not supported)

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet spark

Elapsed: 1163ms

### Rust Arrow reader (through JNI)

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar -Djava.library.path=./src/main/rust/parquetlib/target/debug org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet jni

Elapsed: 290.09ms