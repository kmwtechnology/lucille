package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * A stage for getting a file's contents (array of bytes) using a FileContentFetcher.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>filePathField (String, Optional) : The document field that contains the file path. Defaults to "file_path". No processing will
 *   take place on documents that do not have this field.</li>
 *   <li>fileContentField (String, Optional) : The document field to write the contents to. Defaults to "file_content". This stage will
 *   overwrite any contents associated with this field.</li>
 *   <li>s3 (Map, Optional) : Add if you will be fetching contents from S3 files. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>azure (Map, Optional) : Add if you will be fetching contents from Azure files. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>gcp (Map, Optional) : Add if you will be fetching contents from Google cloud. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>maxSizeBytes (Long, Optional) : The maximum size, in bytes, of a file this stage will load. Must be positive and at most Integer.MAX_VALUE - 9. Defaults to
 *   unlimited. When set, at most maxSizeBytes + 1 bytes are read from a file; if the file is larger than the limit, a StageException
 *   is thrown (as for any other failure to get a file's contents) and the document's content field is left untouched.</li>
 * </ul>
 * The stream opened for each file is always closed after reading.
 */
public class FetchFileContent extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("filePathField", "fileContentField")
      .optionalNumber("maxSizeBytes")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC).build();

  private final String filePathField;
  private final String fileContentField;

  private static final long MAX_ALLOWED_SIZE_BYTES = Integer.MAX_VALUE - 9L;

  private final Long maxSizeBytes;

  private final FileContentFetcher fileFetcher;

  public FetchFileContent(Config config) {
    this(config, FileContentFetcher.create(config));
  }

  // Allows tests to supply a fetcher.
  FetchFileContent(Config config, FileContentFetcher fileFetcher) {
    super(config);

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileContentField = ConfigUtils.getOrDefault(config, "fileContentField", "file_content");

    this.maxSizeBytes = config.hasPath("maxSizeBytes") ? config.getLong("maxSizeBytes") : null;
    if (maxSizeBytes != null && maxSizeBytes <= 0) {
      throw new IllegalArgumentException("maxSizeBytes must be positive, but was " + maxSizeBytes);
    }
    // limit + 1 bytes must fit in a byte[], otherwise an oversized file would be silently truncated
    if (maxSizeBytes != null && maxSizeBytes > MAX_ALLOWED_SIZE_BYTES) {
      throw new IllegalArgumentException("maxSizeBytes must be at most " + MAX_ALLOWED_SIZE_BYTES + ", but was " + maxSizeBytes);
    }

    this.fileFetcher = fileFetcher;
  }

  @Override
  public void start() throws StageException {
    try {
      fileFetcher.startup();
    } catch (IOException e) {
      throw new StageException("Error starting up FileContentFetcher.", e);
    }
  }

  @Override
  public void stop() throws StageException {
    fileFetcher.shutdown();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);

    try (InputStream fileContentStream = fileFetcher.getInputStream(filePath)) {
      byte[] fileContents;
      if (maxSizeBytes == null) {
        fileContents = fileContentStream.readAllBytes();
      } else {
        // read one byte past the limit so an oversized file is detected without loading all of it
        fileContents = fileContentStream.readNBytes((int) (maxSizeBytes + 1));
        if (fileContents.length > maxSizeBytes) {
          throw new StageException("File " + filePath + " exceeds maxSizeBytes of " + maxSizeBytes + ".");
        }
      }
      doc.setField(fileContentField, fileContents);
    } catch (IOException e) {
      throw new StageException("Error occurred while getting document's contents.", e);
    }

    return null;
  }
}
