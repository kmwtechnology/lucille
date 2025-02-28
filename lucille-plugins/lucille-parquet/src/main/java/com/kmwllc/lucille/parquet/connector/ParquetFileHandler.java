package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.BaseFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.InputFile;

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
   *
   * <br> <b>NOTE:</b> The processFile method temporarily loads the provided inputStream's contents into memory for processing.
   * Ensure you have ample resources to process the files you are providing to this handler.
   */
  public ParquetFileHandler(Config config) {
    super(config);

    this.idField = config.getString("idField");
    this.start = ConfigUtils.getOrDefault(config, "start", 0L);
    this.limit = ConfigUtils.getOrDefault(config, "limit", -1L);
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    try {
      InputFile byteInputFile = new ByteArrayInputFile(inputStream.readAllBytes());
      ParquetFileReader reader = ParquetFileReader.open(byteInputFile);
      return new ParquetFileIterator(reader, idField, start, limit);
    } catch (IOException e) {
      throw new FileHandlerException("Error occurred trying to process Parquet file.", e);
    }
  }
}
