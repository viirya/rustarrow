package org.viirya.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ParquetReaderUtils {

  public static Parquet getParquetData(String filePath) throws IOException {
    List<SimpleGroup> simpleGroups = new ArrayList<>();
    ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromPath(new Path(filePath), new Configuration()));
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    List<Type> fields = schema.getFields();
    PageReadStore pages;
    while ((pages = reader.readNextRowGroup()) != null) {
      long rows = pages.getRowCount();
      MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
      RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

      for (int i = 0; i < rows; i++) {
        SimpleGroup simpleGroup = (SimpleGroup) recordReader.read();
        simpleGroups.add(simpleGroup);
      }
    }
    reader.close();
    return new Parquet(simpleGroups, fields);
  }
}
