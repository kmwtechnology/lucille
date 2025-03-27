package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigSpec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;

/**
 * A stage for getting a file's contents (array of bytes) using a FileContentFetcher.
 *
 * filePathField (String, Optional): The document field that contains the file path. Defaults to "file_path". No processing will
 * take place on documents that do not have this field.
 * fileContentField (String, Optional): The document field to write the contents to. Defaults to "file_content".
 * This stage will overwrite any contents associated with this field.
 *
 * s3 (Map, Optional): Add if you will be fetching contents from S3 files. See FileConnector for the appropriate arguments to provide.
 * azure (Map, Optional): Add if you will be fetching contents from Azure files. See FileConnector for the appropriate arguments to provide.
 * gcp (Map, Optional): Add if you will be fetching contents from Google cloud. See FileConnector for the appropriate arguments to provide.
 */
public class FetchFileContent extends Stage {

  private final String filePathField;
  private final String fileContentField;

  private final FileContentFetcher fileFetcher;

  public FetchFileContent(Config config) {
    super(config, new ConfigSpec().withOptionalProperties("filePathField", "fileContentField")
        .withOptionalParents("s3", "azure", "gcp"));

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileContentField = ConfigUtils.getOrDefault(config, "fileContentField", "file_content");

    this.fileFetcher = new FileContentFetcher(config);
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

    try {
      InputStream fileContentStream = fileFetcher.getInputStream(filePath);
      byte[] fileContents = fileContentStream.readAllBytes();
      doc.setField(fileContentField, fileContents);
    } catch (IOException e) {
      throw new StageException("Error occurred while getting document's contents.", e);
    }

    return null;
  }
}
