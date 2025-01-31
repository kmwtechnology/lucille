package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.net.URI;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroReadSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ParquetFileHandler extends BaseFileHandler {

  private static final Logger log = LoggerFactory.getLogger(ParquetFileHandler.class);

  private final String idField;
  private final String fsUri;

  private final Configuration configuration;

  private final long limit;
  private final long start;

  public ParquetFileHandler(Config config) {
    super(config);

    String s3Key = ConfigUtils.getOrDefault(config, "s3_key", null);
    String s3Secret = ConfigUtils.getOrDefault(config, "s3_secret", null);

    this.idField = config.getString("id_field");
    this.fsUri = config.getString("fs_uri");

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
    try (FileSystem fs = FileSystem.get(new URI(fsUri), configuration)) {
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new org.apache.hadoop.fs.Path(javaPath.toUri()), true);

      return new ParquetFileIterator(statusIterator, configuration, idField, start, limit);
    } catch (Exception e) {
      throw new FileHandlerException("Problem running processFile", e);
    }
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }
}
