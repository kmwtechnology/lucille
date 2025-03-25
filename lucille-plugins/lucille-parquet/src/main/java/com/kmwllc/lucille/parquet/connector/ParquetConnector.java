package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.parquet.ParquetFileIterator;
import com.typesafe.config.Config;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;

import java.net.URI;

/**
 * A connector for processing Parquet files, either locally, or on S3, and publishing Lucille documents from
 * files.
 */
public class ParquetConnector extends AbstractConnector {

  private final String path;
  private final String idField;
  private final String fsUri;
  private final Configuration hadoopConfig;

  // The row to start at in files. Can also think of this as a number of documents to skip (from the beginning of the file).
  private final long start;
  private final long limit;

  private long count = 0L;

  /**
   * Creates a ParquetConnector from the given config.
   * @param config Configuration for the ParquetConnector.
   */
  public ParquetConnector(Config config) {
    super(config);
    this.path = config.getString("pathToStorage");
    this.idField = config.getString("id_field");
    this.fsUri = config.getString("fs_uri");

    this.limit = config.hasPath("limit") ? config.getLong("limit") : -1;
    this.start = config.hasPath("start") ? config.getLong("start") : 0L;

    this.hadoopConfig = new Configuration();

    if (config.hasPath("s3_key") && config.hasPath("s3_secret")) {
      hadoopConfig.set("fs.s3a.access.key", config.getString("s3_key"));
      hadoopConfig.set("fs.s3a.secret.key", config.getString("s3_secret"));
      hadoopConfig.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      hadoopConfig.setBoolean("fs.s3a.path.style.access", true);
      hadoopConfig.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
    }
  }

  private boolean limitNotReached() {
    return limit < 0 || count < limit;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try (FileSystem fs = FileSystem.get(new URI(fsUri), hadoopConfig)) {
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new Path(path), true);

      while (limitNotReached() && statusIterator.hasNext()) {
        LocatedFileStatus status = statusIterator.next();
        // only processing parquet files
        if (!status.getPath().getName().endsWith("parquet")) {
          continue;
        }

        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(status, hadoopConfig));
        Iterator<Document> docIterator = new ParquetFileIterator(reader, idField, start, limit - count);

        while (docIterator.hasNext()) {
          Document doc = docIterator.next();

          if (doc != null) {
            publisher.publish(doc);
            count++;
          }
        }
      }
    } catch (Exception e) {
      throw new ConnectorException("Problem running the ParquetConnector", e);
    }
  }
}
