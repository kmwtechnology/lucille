package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

public class ParquetFileHandler extends BaseFileHandler {

  private final String idField;

  private final long limit;
  private final long start;

  /**
   * Handles processing of Parquet Files.
   *
   * <br> Params:
   * <br> - <b>idField</b> (String): The name of an id field which will be found in the Parquet documents that are processed
   *        by this handler. An exception is thrown if a Parquet document does not have this field in its schema.
   * <br> - <b>start</b> (Int, Optional): The number of rows to initially skip in each Parquet file. Defaults to zero.
   * <br> - <b>limit</b> (Int, Optional): The maximum number of Documents to extract from each Parquet file. Set to -1
   *        for no limit. Defaults to -1.
   */
  public ParquetFileHandler(Config config) {
    super(config);

    this.idField = config.getString("idField");
    this.start = ConfigUtils.getOrDefault(config, "start", 0L);
    this.limit = ConfigUtils.getOrDefault(config, "limit", -1);
  }

  @Override
  public Iterator<Document> processFile(java.nio.file.Path javaPath) throws FileHandlerException {
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(javaPath.toString());

      Configuration hadoopConf = new Configuration();
      hadoopConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

      HadoopInputFile hadoopFile = HadoopInputFile.fromPath(hadoopPath, hadoopConf);
      ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

      return new ParquetFileIterator(reader, idField, start, limit);
    } catch (Exception e) {
      throw new FileHandlerException("Problem running processFile: ", e);
    }
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException {
    try {
      org.apache.hadoop.fs.Path hadoopPath = new org.apache.hadoop.fs.Path(pathStr);

      Configuration hadoopConf = new Configuration();
      hadoopConf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);

      HadoopInputFile hadoopFile = HadoopInputFile.fromPath(hadoopPath, hadoopConf);
      ParquetFileReader reader = ParquetFileReader.open(hadoopFile);

      return new ParquetFileIterator(reader, idField, start, limit);
    } catch (Exception e) {
      throw new FileHandlerException("Problem running processFile: ", e);
    }
  }
}
