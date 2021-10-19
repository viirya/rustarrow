## Microbenchmarking reading parquet file

### Parquet reader

    mvn package 
    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet raw

Elapsed: 831ms

### Spark vectorized Parquet reader (one column only because complex type is not supported)

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet spark

Elapsed: 1163ms

### Rust Arrow reader (through JNI)

Java calls Rust to read Parquet completely. Before running the reader, please build Rust library first.

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar -Djava.library.path=./src/main/rust/parquetlib/target/debug org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet native

Elapsed: 290.09ms

### Rust Arrow reader (through JNI) + Java

Java calls Rust to read Parquet and returns Arrow vectors. Java then iterates returned Arrow vectors. Before running the reader, please build Rust library first.
This needs Java implementation of [Arrow C data interface](https://issues.apache.org/jira/browse/ARROW-12965) installed.

    java -cp target/parquet_reader-1.0-SNAPSHOT.jar -Djava.library.path=./src/main/rust/parquetlib/target/debug org.viirya.parquet.ParquetReader ../rust/parquet_read/data/simple_data.parquet jni

Elapsed: 507ms

