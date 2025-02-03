package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileHandler extends BaseFileHandler {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileHandler.class);

  private final String idField;

  private final long limit;
  private final long start;

  public ParquetFileHandler(Config config) {
    super(config);

    this.idField = config.getString("id_field");
    this.start = ConfigUtils.getOrDefault(config, "start", 0L);
    this.limit = ConfigUtils.getOrDefault(config, "limit", -1);
  }

  @Override
  public Iterator<Document> processFile(java.nio.file.Path javaPath) throws FileHandlerException {
    try {
      Path hadoopPath = new Path(javaPath.toString());
      // TODO: Add AVROREADSUPPORT.READ_INT96... back to the config?
      HadoopInputFile hadoopFile = HadoopInputFile.fromPath(hadoopPath, new Configuration());
      ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

      return new ParquetFileIterator(reader, idField, start, limit);
    } catch (Exception e) {
      throw new FileHandlerException("Problem running processFile", e);
    }
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }
}
