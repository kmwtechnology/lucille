package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileHandler extends BaseFileHandler {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileHandler.class);

  private final String idField;

  private final Configuration configuration;

  private final long limit;

  // non final
  private long count = 0L;
  private long start;

  public ParquetFileHandler(Config config) {
    super(config);

    String s3Key = ConfigUtils.getOrDefault(config, "s3_key", null);
    String s3Secret = ConfigUtils.getOrDefault(config, "s3_secret", null);

    this.idField = config.getString("id_field");

    this.configuration = new Configuration();

    if (s3Key != null && s3Secret != null) {
      configuration.set("fs.s3a.access.key", s3Key);
      configuration.set("fs.s3a.secret.key", s3Secret);
      configuration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      configuration.setBoolean("fs.s3a.path.style.access", true);
      configuration.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
    }

    this.start = ConfigUtils.getOrDefault(config, "start", 0L);
    this.limit = ConfigUtils.getOrDefault(config, "limit", -1);
  }

  @Override
  public Iterator<Document> processFile(java.nio.file.Path javaPath) throws FileHandlerException {
    try (FileSystem fs = FileSystem.get(javaPath.toUri(), configuration)) {
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new org.apache.hadoop.fs.Path(javaPath.toUri()), true);

      return getDocumentIterator(statusIterator);
    } catch (Exception e) {
      throw new FileHandlerException("Problem running processFile", e);
    }
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsuppored Operation");
  }

  private boolean canSkipAndUpdateStart(long n) {
    if (start > n) {
      start -= n;
      return true;
    }
    return false;
  }

  private static void setDocField(Document doc, Type field, SimpleGroup simpleGroup, int j) {
    // check if we have a field value before setting it.
    if (simpleGroup.getFieldRepetitionCount(j) == 0) {
      return;
    }

    String fieldName = field.getName();

    switch (field.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.setField(fieldName, simpleGroup.getString(j, 0));
        break;
      case FLOAT:
        doc.setField(fieldName, simpleGroup.getFloat(j, 0));
        break;
      case INT32:
        doc.setField(fieldName, simpleGroup.getInteger(j, 0));
        break;
      case INT64:
        doc.setField(fieldName, simpleGroup.getLong(j, 0));
        break;
      case DOUBLE:
        doc.setField(fieldName, simpleGroup.getDouble(j, 0));
        break;
      // todo consider adding a default case
    }
  }

  private static void addToField(Document doc, String fieldName, Type type, Group group) {
    switch (type.asPrimitiveType().getPrimitiveTypeName()) {
      case BINARY:
        doc.addToField(fieldName, group.getString(0, 0));
        break;
      case FLOAT:
        doc.addToField(fieldName, group.getFloat(0, 0));
        break;
      case INT32:
        doc.addToField(fieldName, group.getInteger(0, 0));
        break;
      case INT64:
        doc.addToField(fieldName, group.getLong(0, 0));
        break;
      case DOUBLE:
        doc.addToField(fieldName, group.getDouble(0, 0));
        break;
      // todo consider adding a default case
    }
  }

  private Iterator<Document> getDocumentIterator(RemoteIterator<LocatedFileStatus> statusIterator) {

    return new Iterator<Document>() {
      private LocatedFileStatus currentStatus;

      @Override
      public boolean hasNext() {
        // First, just check if limit not reached.
        if (!limitNotReached()) {
          return false;
        }

        return nrows-- > 0
            || pages = reader.readNextRowGroup() != null
            || statusIterator.hasNext();
        // Is nRows -- > 0?

        // Is pages = reader.readNextRowGroup() != null?

        // does statusIterator have a next? Continue checking until we get one that endsWith "parquet"
        // (how to do that without mutation... i don't know... a stream?)

        try {
          return limitNotReached() && statusIterator.hasNext();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public Document next() {
        try {
          currentStatus = statusIterator.next();

          try (ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(currentStatus, configuration))) {

          }

          // It should be okay to just return null in the event of a document that we are skipping.
          // (e.g. a file that doesn't end in parquet.)
        } catch (IOException e) {
          throw new RuntimeException(e);
        }

        return null;
      }

      private boolean limitNotReached() {
        return limit < 0 || count < limit;
      }
    };

  }
}
