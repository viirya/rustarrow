/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use arrow::record_batch::RecordBatchReader;
use log::info;
use parquet::file::reader::SerializedFileReader;
use parquet::arrow::{ParquetFileArrowReader, ArrowReader};

use std::ffi::CStr;
use std::fs::File;
use std::str;
use std::sync::Arc;
use std::os::raw::c_char;

#[repr(C)]
#[allow(missing_copy_implementations)]
pub struct RustArrow {
    // A pointer to the beginning of a string. Converted to a Java String by JNA. All strings
    // passed between Rust and Java are represented in this way.
    text: *const c_char
}

#[no_mangle]
#[allow(non_snake_case)]
pub extern fn readParquetFile(filename: *const c_char) {
    // Convert the C string to a Rust one
    let parquet_filename = to_string(filename);
    info!("Hello from Rust, {}", parquet_filename);

    let file = File::open(parquet_filename).unwrap();
    let file_reader = SerializedFileReader::new(file).unwrap();
    let mut arrow_reader = ParquetFileArrowReader::new(Arc::new(file_reader));

    info!("Converted arrow schema is: {}", arrow_reader.get_schema().unwrap());

    let mut record_batch_reader = arrow_reader.get_record_reader(2048).unwrap();

    for maybe_record_batch in record_batch_reader {
        let record_batch = maybe_record_batch.unwrap();
        if record_batch.num_rows() > 0 {
            info!("Read {} records.", record_batch.num_rows());
        } else {
            info!("End of file!");
        }
    }
}

/// Convert a native string to a Rust string
fn to_string(pointer: *const c_char) -> String {
    let slice = unsafe { CStr::from_ptr(pointer).to_bytes() };
    str::from_utf8(slice).unwrap().to_string()
}