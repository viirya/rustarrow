
use arrow::array::{Int32Array, StructArray};
use arrow::datatypes::Int32Type;
use arrow::record_batch::RecordBatchReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};
use parquet::file::reader::SerializedFileReader;
use parquet::record::reader::RowIter;

use std::env;
use std::fs::File;
use std::sync::Arc;
use std::time::Instant;
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
    let now = Instant::now();

    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();

    let rows: RowIter = file_reader.into_iter();

    let mut accu1: i64 = 0;
    let mut accu2: i64 = 0;
    for row in rows {
        accu1 += row.get_int(0).unwrap() as i64;
        accu2 += row.get_group(3).unwrap().get_int(0).unwrap() as i64;
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    println!("{}", accu1);
    println!("{}", accu2);
}

pub fn read_parquet_file_to_arrow(parquet_filename: &String) {
    let now = Instant::now();

    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());

    let record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

    let mut accu1: i64 = 0;
    let mut accu2: i64 = 0;

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        if record_batch.num_rows() > 0 {
            let column1 = record_batch.column(0).as_any().downcast_ref::<Int32Array>();
            column1.unwrap().values().iter().for_each(|i| {
                accu1 += *i as i64
            });

            let column2 = record_batch.column(3).as_any().downcast_ref::<StructArray>();

            column2.unwrap().column(0).as_any().downcast_ref::<Int32Array>().unwrap().values().iter().for_each(|i| {
                accu2 += *i as i64
            });
        } else {
            println!("End of file!");
        }
    }

    let elapsed = now.elapsed();
    println!("Elapsed: {:.2?}", elapsed);

    println!("{}", accu1);
    println!("{}", accu2);
}