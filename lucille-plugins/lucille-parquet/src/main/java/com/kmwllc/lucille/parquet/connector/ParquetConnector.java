package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.net.URI;
import java.nio.file.Paths;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
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

public class ParquetConnector extends AbstractConnector {
  private final String pathStr;
  private final String fsUri;

  private final ParquetFileHandler parquetFileHandler;

  private static final Logger log = LoggerFactory.getLogger(ParquetConnector.class);

  public ParquetConnector(Config config) {
    super(config);
    this.pathStr = config.getString("path");
    this.fsUri = config.getString("fs_uri");
    this.parquetFileHandler = new ParquetFileHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    java.nio.file.Path javaPath;

    try (FileSystem fs = FileSystem.get(new URI(fsURI), conf)) {

    }

    try {
      javaPath = Paths.get(pathStr);
    } catch (Exception e) {
      throw new ConnectorException("Error creating path from String " + pathStr, e);
    }

    try {
      log.debug("Processing file: {}", javaPath);
      parquetFileHandler.processFileAndPublish(publisher, javaPath);
    } catch (Exception e) {
      throw new ConnectorException("Error processing or publishing file: " + javaPath, e);
    }
  }
}
