package com.kmwllc.lucille.parquet.core.fileHandler;

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
  private final long numToSkip;

  /**
   * Handles processing of Parquet Files.
   *
   * <br> Params:
   * <br> - <b>idField</b> (String): An id field which must be found in the Parquet records that are processed
   *        by this handler. An exception is thrown while iterating if this field is not in the file's schema.
   * <br> - <b>numToSkip</b> (Int, Optional): The number of rows in the beginning of each Parquet file to not publish Documents for. Defaults to zero.
   * <br> - <b>limit</b> (Int, Optional): The maximum number of Documents to extract from each Parquet file. Set to -1
   *        for no limit. Defaults to -1.
   *
   * <br> <b>NOTE:</b> The processFile method temporarily loads the provided inputStream's contents into memory for processing.
   * Ensure you have ample resources to process the files you are providing to this handler.
   */
  public ParquetFileHandler(Config config) {
    super(config);

    this.idField = config.getString("idField");
    this.numToSkip = ConfigUtils.getOrDefault(config, "numToSkip", 0L);
    this.limit = ConfigUtils.getOrDefault(config, "limit", -1L);
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    try {
      InputFile byteInputFile = new ByteArrayInputFile(inputStream.readAllBytes());
      ParquetFileReader reader = ParquetFileReader.open(byteInputFile);
      return new ParquetFileIterator(reader, idField, numToSkip, limit);
    } catch (IOException e) {
      throw new FileHandlerException("Error occurred trying to process Parquet file.", e);
    }
  }
}
