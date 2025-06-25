package com.kmwllc.lucille.parquet.connector;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.spec.Spec;
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
 *
 * <p> Config Parameters:
 * <ul>
 *   <li>pathToStorage (String): The path to storage you want to traverse for <code>.parquet</code> files.</li>
 *   <li>id_field (String): The name of a field found in the Parquet files you will process that can be used for Document IDs.
 *   Must be found in the file's schema, or an Exception will be thrown.</li>
 *   <li>fs_uri (String): A URI for the file system that you want to use (for your <code>pathToStorage</code>).</li>
 *   <li>s3_key (String, Optional): Your key to AWS S3. Only needed if using S3.</li>
 *   <li>s3_secret (String, Optional): Your secret to AWS S3. Only needed if using S3.</li>
 *   <li>limit (Long, Optional): The maximum number of Documents to publish. Defaults to no limit.</li>
 *   <li>start (Long, Optional): The number of rows to skip from the beginning of each parquet file encountered. Defaults to skipping no rows.</li>
 * </ul>
 *
 * <b>Note:</b> If you are paginating (using start / limit), it is recommended you use individual Connectors for each Parquet file.
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

  public static final Spec SPEC = Spec.connector()
      .requiredString("pathToStorage", "id_field", "fs_uri")
      .optionalString("s3_key", "s3_secret")
      .optionalNumber("limit", "start");

  public ParquetConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("path", "id_field", "fs_uri")
        .withOptionalProperties("s3_key", "s3_secret", "limit", "start"));

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
