package org.viirya.parquet;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.List;

import org.apache.parquet.example.data.simple.SimpleGroup;

public class ParquetReader {

  public static void main(String[] args) throws IOException  {
    List<String> arguments = asList(args);
    if (arguments.isEmpty()) {
      System.out.println("ParquetReader takes one parquet filename as input");
      return;
    } else {
      long startTime = System.currentTimeMillis();

      String parquetFilename = arguments.get(0);

      System.out.println("parquet filename: " + parquetFilename);

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
    }
  }
}
