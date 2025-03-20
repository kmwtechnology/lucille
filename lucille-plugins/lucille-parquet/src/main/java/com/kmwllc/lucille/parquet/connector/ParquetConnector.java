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

public class ParquetConnector extends AbstractConnector {

  private final String path;
  private final String idField;
  private final String fsUri;


  private final String s3Key;
  private final String s3Secret;

  // non final
  private long start;
  private long limit;
  private long count = 0L;


  public ParquetConnector(Config config) {
    super(config);
    this.path = config.getString("pathToStorage");
    this.idField = config.getString("id_field");
    this.fsUri = config.getString("fs_uri");

    this.s3Key = config.hasPath("s3_key") ? config.getString("s3_key") : null;
    this.s3Secret = config.hasPath("s3_secret") ? config.getString("s3_secret") : null;
    this.limit = config.hasPath("limit") ? config.getLong("limit") : -1;
    this.start = config.hasPath("start") ? config.getLong("start") : 0L;
  }

  private boolean limitNotReached() {
    return limit < 0 || count < limit;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    Configuration conf = new Configuration();
    if (s3Key != null && s3Secret != null) {
      conf.set("fs.s3a.access.key", s3Key);
      conf.set("fs.s3a.secret.key", s3Secret);
      conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
      conf.setBoolean("fs.s3a.path.style.access", true);
      conf.setBoolean(AvroReadSupport.READ_INT96_AS_FIXED, true);
    }

    try (FileSystem fs = FileSystem.get(new URI(fsUri), conf)) {
      RemoteIterator<LocatedFileStatus> statusIterator = fs.listFiles(new Path(path), true);
      while (limitNotReached() && statusIterator.hasNext()) {
        LocatedFileStatus status = statusIterator.next();
        //only process parquet files
        if (!status.getPath().getName().endsWith("parquet")) {
          continue;
        }

        ParquetFileReader reader = ParquetFileReader.open(HadoopInputFile.fromStatus(status, conf));
        // start gets updated as we publish individual documents in the connector.
        // we want to extract only up to the "remaining" limit from any one file.
        Iterator<Document> docIterator = new ParquetFileIterator(reader, idField, start, limit - count);

        while (docIterator.hasNext()) {
          Document doc = docIterator.next();

          if (doc != null) {
            publisher.publish(doc);
            count++;

            if (start > 0) {
              start--;
            }
          }
        }
      }
    } catch (Exception e) {
      throw new ConnectorException("Problem running the ParquetConnector", e);
    }
  }
}
