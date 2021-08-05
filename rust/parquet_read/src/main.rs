
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;

use std::env;
use std::fs::File;
use std::sync::Arc;
use parquet::record::RowAccessor;

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() <= 1 {
        println!("parquet_read takes one parquet filename as input.");
    } else {
        let filename = &args[1];
        println!("filename: {:?}", filename);

        if args.len() > 2 {
            let method = &args[2];
            match method.as_str() {
               "arrow" => read_parquet_file_to_arrow(filename),
               "raw" => read_parquet_file(filename),
                _ => print!("unknowen method: {}", method)
            }
        } else {
            read_parquet_file(filename);
        }
    }
}

pub fn read_parquet_file(parquet_filename: &String) {
    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();

    let rows: RowIter = file_reader.into_iter();

    let mut accu1: i64 = 0;
    let mut accu2: i64 = 0;
    for row in rows {
        accu1 += row.get_int(0).unwrap() as i64;
        accu2 += row.get_group(3).unwrap().get_int(0).unwrap() as i64;
    }
    println!("{}", accu1);
    println!("{}", accu2);
}

pub fn read_parquet_file_to_arrow(parquet_filename: &String) {
    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());

    let record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        if record_batch.num_rows() > 0 {
            println!("Read {} records.", record_batch.num_rows());
        } else {
            println!("End of file!");
        }
    }
}