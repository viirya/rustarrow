// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JString};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::jstring;

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

// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_org_viirya_parquet_ParquetNative_loadParquetFile(env: JNIEnv,
// This is the class that owns our static method. It's not going to be used,
// but still must be present to match the expected signature of a static
// native method.
                                             class: JClass,
                                             input: JString)
                                             -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String =
        env.get_string(input).expect("Couldn't get java string!").into();

    read_parquet_file_to_arrow(&input);

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env.new_string(format!("Hello, {}!", input))
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_inner()
}

pub fn read_parquet_file_to_arrow(parquet_filename: &String) {
    let now = Instant::now();

    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    println!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());

    let record_batch_reader = arrow_reader.get_record_reader(65536).unwrap();

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
