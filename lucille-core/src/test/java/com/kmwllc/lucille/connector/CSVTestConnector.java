package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.filehandler.CSVFileHandler;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.InputStream;
import java.util.List;

/**
 * A connector for tests that publishes the rows of one CSV file as documents, using {@link CSVFileHandler}. Unlike
 * FileConnector it accepts a {@code classpath:} path, so tests can read CSV fixtures from their resources.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>path (String, Required) : Path to the CSV file, which may start with {@code classpath:}.</li>
 *   <li>Any other {@link CSVFileHandler} setting (idField, separatorChar, docIdPrefix, ...), passed to the handler.</li>
 * </ul>
 */
public class CSVTestConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("path")
      .optionalString("lineNumberField", "filenameField", "filePathField", "idField", "docIdFormat", "separatorChar")
      .optionalBoolean("useTabs", "interpretQuotes", "ignoreEscapeChar", "lowercaseFields")
      .optionalNumber("maxRecordBytes")
      .optionalList("ignoredTerms", new TypeReference<List<String>>(){}).build();

  private final String path;
  private final CSVFileHandler csvFileHandler;

  public CSVTestConnector(Config config) {
    super(config);
    this.path = config.getString("path");
    this.csvFileHandler = new CSVFileHandler(config
        .withoutPath("path")
        .withoutPath("name")
        .withoutPath("pipeline")
        .withoutPath("collapse"));
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try (InputStream stream = FileContentFetcher.getOneTimeInputStream(path)) {
      csvFileHandler.processFileAndPublish(publisher, stream, path);
    } catch (Exception e) {
      throw new ConnectorException("Error processing or publishing file: " + path, e);
    }
  }
}
