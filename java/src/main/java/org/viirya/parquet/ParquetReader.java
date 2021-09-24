package org.viirya.parquet;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.ffi.FFI;
import org.apache.arrow.ffi.ArrowArray;
import org.apache.arrow.ffi.ArrowSchema;
import org.apache.arrow.ffi.FFIDictionaryProvider;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.spark.sql.vectorized.ArrowColumnVector;
import org.apache.spark.sql.vectorized.ColumnVector;
import org.apache.spark.sql.vectorized.ColumnarBatch;
import org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader;

public class ParquetReader {

  public static void main(String[] args) throws IOException {
    List<String> arguments = asList(args);
    if (arguments.isEmpty()) {
      System.out.println("ParquetReader takes one parquet filename as input");
      return;
    } else {
      String parquetFilename = arguments.get(0);

      System.out.println("parquet filename: " + parquetFilename);

      String approach;
      if (arguments.size() == 1) {
        approach = "raw";
      } else {
        approach = arguments.get(1);
      }

      if (approach.equals("raw")) {
        long startTime = System.currentTimeMillis();

        Parquet parquet = ParquetReaderUtils.getParquetData(parquetFilename);

        int numRows = parquet.getData().size();

        long accu1 = 0;
        long accu2 = 0;

        for (int i = 0; i < numRows; i++) {
          SimpleGroup simpleGroup = parquet.getData().get(i);

          accu1 += simpleGroup.getInteger(0, 0);
          accu2 += simpleGroup.getGroup(3, 0).getInteger(0, 0);
        }

        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed: " + (endTime - startTime) + "ms");
        System.out.println(accu1);
        System.out.println(accu2);
      } else if (approach.equals("spark")) {
        read_parquet_using_spark_reader(parquetFilename);
      } else if (approach.equals("jni")) {
        long startTime = System.currentTimeMillis();
        long[] arrayAddress = ParquetNative.loadParquetFileAsArrow(parquetFilename);
        read_parquet_from_native_arrow_arrays(arrayAddress);
        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed: " + (endTime - startTime) + "ms");
      } else if (approach.equals("native")) {
        long startTime = System.currentTimeMillis();
        String output = ParquetNative.loadParquetFile(parquetFilename);
        System.out.println(output);
        long endTime = System.currentTimeMillis();
        System.out.println("Elapsed: " + (endTime - startTime) + "ms");
      } else {
        System.out.println("Unknown method: " + approach);
      }
    }
  }

  static void read_parquet_from_native_arrow_arrays(long[] arrayAddress) {
    RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
    FFIDictionaryProvider ffiDictionaryProvider = new FFIDictionaryProvider();

    long accu = 0;
    for (int i = 0; i < arrayAddress.length; i += 2) {
      try (ArrowSchema arrowSchema = ArrowSchema.wrap(arrayAddress[i + 1]);
           ArrowArray arrowArray = ArrowArray.wrap(arrayAddress[i])) {
        FieldVector imported = FFI.importVector(allocator, arrowArray, arrowSchema, ffiDictionaryProvider);
        int rowCount = imported.getValueCount();
        System.out.println("rowCount: " + rowCount);

        for (int j = 0; j < rowCount; j++) {
          if (imported instanceof IntVector) {
            IntVector vector = ((IntVector) imported);
            accu += vector.get(j);
          } else if (imported instanceof BigIntVector) {
            BigIntVector vector = ((BigIntVector) imported);
            accu += vector.get(j);
          }
        }
      }
    }
    System.out.println("accu1 = " + accu);
  }

  static void read_parquet_using_spark_reader(String filePath) throws IOException {
    long startTime = System.currentTimeMillis();

    VectorizedParquetRecordReader reader = new VectorizedParquetRecordReader(true, 65536);

    try {
      List<String> columns = new ArrayList<String>();
      columns.add("a");

      reader.initialize(filePath, columns);
      ColumnarBatch batch = reader.resultBatch();
      ColumnVector col1 = batch.column(0);
      // Complex type is not supported.
      // ColumnVector col2 = batch.column(3).getChild(0);

      long accu1 = 0;
      while (reader.nextBatch()) {
        int numRows = batch.numRows();
        System.out.println("num rows: " + numRows);

        int i = 0;
        while (i < numRows) {
          accu1 += col1.getInt(i);

          i += 1;
        }
      }
      long endTime = System.currentTimeMillis();
      System.out.println("Elapsed: " + (endTime - startTime) + "ms");
      System.out.println(accu1);
    } finally {
      reader.close();
    }
  }
}


