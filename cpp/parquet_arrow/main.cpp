#include <chrono>
#include <iostream>
#include <arrow/api.h>
#include <arrow/result.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/exception.h>

using std::chrono::high_resolution_clock;
using std::chrono::duration_cast;
using std::chrono::duration;
using std::chrono::milliseconds;

void read_parquet_file(std::string parquet_filename) {
    std::cout << "Reading " << parquet_filename << std::endl;
    arrow::Result<std::shared_ptr<arrow::io::ReadableFile>> infileResult = arrow::io::ReadableFile::Open(
            parquet_filename, arrow::default_memory_pool());
    PARQUET_THROW_NOT_OK(infileResult);

    std::shared_ptr<arrow::io::ReadableFile> infile = infileResult.ValueUnsafe();

    std::unique_ptr<parquet::arrow::FileReader> reader;
    PARQUET_THROW_NOT_OK(
            parquet::arrow::OpenFile(infile, arrow::default_memory_pool(), &reader));

    std::unique_ptr<arrow::RecordBatchReader> recordBatchReader;

    std::vector<int> rowGroup;
    for(int i = 0; i < reader->num_row_groups(); i++) {
        rowGroup.push_back(i);
    }
    PARQUET_THROW_NOT_OK(reader->GetRecordBatchReader(rowGroup, &recordBatchReader));

    long accu1 = 0;
    long accu2 = 0;
    while (true) {
        arrow::Result<std::shared_ptr<arrow::RecordBatch>> next = recordBatchReader->Next();

        if (next.ok()) {
            std::shared_ptr<arrow::RecordBatch> nextRecordBatch = next.ValueUnsafe();
            // Even `Next` result is okay, the record batch could possible be null...
            if (nextRecordBatch != NULL) {
                std::shared_ptr<arrow::ArrayData> data = next.ValueOrDie()->column(0)->data();
                int num_rows = data->length;

                std::cout << "num rows: " << num_rows << std::endl;
                for (int i = 0; i < num_rows; ++i) {
                    // Read an int (column 0)
                    std::shared_ptr<arrow::Scalar> scalar = next.ValueOrDie()->column(0)->GetScalar(i).ValueUnsafe();
                    // std::cout << scalar->ToString() << std::endl;
                    // std::cout << scalar.get()->type.get()->ToString() << std::endl;

                    accu1 += (long) *(int32_t*)((arrow::Int32Scalar*) scalar.get())->data();

                    // Read a struct (column 3)
                    scalar = next.ValueOrDie()->column(3)->GetScalar(i).ValueUnsafe();

                    arrow::StructScalar* structScalar = (arrow::StructScalar*) scalar.get();
                    std::vector<std::shared_ptr<arrow::Scalar>> nestedScalars = structScalar->value;

                    scalar = nestedScalars[0];
                    accu2 += (long) *(int32_t*)((arrow::Int32Scalar*) scalar.get())->data();
                }
                std::cout << "accu1: " << accu1 << std::endl;
                std::cout << "accu2: " << accu2 << std::endl;
            } else {
                return;
            }
        } else {
            return;
        }
    }
}

int main(int argc, char** argv) {
    if (argc == 1) {
        std::cout << "parquet_arrow takes one parquet filename as input." << std::endl;
    } else {
        auto t1 = high_resolution_clock::now();
        read_parquet_file(std::string(argv[1]));
        auto t2 = high_resolution_clock::now();
        auto ms_int = duration_cast<milliseconds>(t2 - t1);
        std::cout << ms_int.count() << "ms" << std::endl;
    }
}
