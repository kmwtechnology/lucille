package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.auto.service.AutoService;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.opencsv.CSVWriterBuilder;
import com.opencsv.ICSVWriter;
import com.typesafe.config.Config;
import java.io.File;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

/**
 * Stores documents in a CSV file by writing selected fields as rows.
 * Additional parameters are made available by the {@link com.kmwllc.lucille.core.Indexer} abstract class.
 * <p>
 * The output file is opened when the indexer connects or first sends documents, not when it is created, so validating
 * a config does not create or overwrite it. In bypass mode (test mode, or indexer.sendEnabled: false) nothing is
 * written.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>path (String, Required) : Output CSV file path.</li>
 *   <li>columns (List&lt;String&gt;, Required) : Ordered list of document fields to write as CSV columns.</li>
 *   <li>includeHeader (Boolean, Optional) : Write a header row with column names when the indexer connects. Defaults to true.</li>
 *   <li>append (Boolean, Optional) : Open the CSV in append mode instead of overwriting. Defaults to false.</li>
 * </ul>
 */
@AutoService(Indexer.class)
@IndexerType("CSV")
public class CSVIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("path")
      .requiredList("columns", new TypeReference<List<String>>(){})
      .optionalBoolean("includeHeader", "append").build();

  private static final Logger log = LoggerFactory.getLogger(CSVIndexer.class);

  // the writer for the output file. An indexer created from config opens the file at path on first use, so that
  // constructing one (for example to validate a config) does not create or truncate it; one given a writer has no path.
  private ICSVWriter writer;
  private final String path;
  private final boolean append;
  private final List<String> columns;
  private final boolean includeHeader;

  /**
   * Creates a CSVIndexer from the given arguments.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   * @param localRunId The runID for a local run, null otherwise.
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, ICSVWriter writer, boolean bypass, String metricsPrefix, String localRunId) {
    this(config, messenger, writer, null, bypass, metricsPrefix, localRunId);
  }

  private CSVIndexer(Config config, IndexerMessenger messenger, ICSVWriter writer, String path, boolean bypass,
      String metricsPrefix, String localRunId) {
    super(config, messenger, bypass, metricsPrefix, localRunId);
    if (this.indexOverrideField != null) {
      // a writer passed in has already opened its output; close it so the file isn't left locked
      if (writer != null) {
        try {
          writer.close();
        } catch (IOException e) {
          log.warn("Error occurred when closing csv indexer writer.", e);
        }
      }
      throw new IllegalArgumentException(
          "Cannot create CSVIndexer. Config setting 'indexer.indexOverrideField' is not supported by CSVIndexer.");
    }
    if (this.deletionMarkerField != null || this.deleteByFieldField != null) {
      log.warn("Deletion is not supported for this indexer. Documents marked for deletion will be written as regular rows.");
    }
    
    this.writer = writer;
    this.path = path;
    this.append = ConfigUtils.getBooleanOrDefault(config, "csv.append", false);
    this.columns = config.getStringList("csv.columns");
    this.includeHeader = ConfigUtils.getBooleanOrDefault(config, "csv.includeHeader", true);
  }

  /**
   * Creates a CSVIndexer from the given arguments.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   * @param localRunId The runID for a local run, null otherwise.
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this(config, messenger, null, config.getString("csv.path"), bypass, metricsPrefix, localRunId);
  }

  // Convenience Constructors - No localRunId needs to be specified, uses "null".

  /**
   * Creates a CSVIndexer from the given arguments with no localRunId.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, ICSVWriter writer, boolean bypass, String metricsPrefix) {
    this(config, messenger, writer, bypass, metricsPrefix, null);
  }

  /**
   * Creates a CSVIndexer from the given arguments with no localRunId.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix) {
    this(config, messenger, null, config.getString("csv.path"), bypass, metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "csv"; }

  /**
   * Returns the writer, opening the output file first if this indexer was created from config and has not opened it
   * yet. Returns null if the file cannot be opened.
   */
  private ICSVWriter getOrOpenWriter() {
    if (writer == null && path != null) {
      try {
        File file = new File(path).getAbsoluteFile();
        if (!file.getParentFile().exists()) {
          file.getParentFile().mkdirs();
        }
        writer = new CSVWriterBuilder(new FileWriter(file, append)).build();
      } catch (IOException e) {
        log.error("Error initializing writer for CSVIndexer", e);
      }
    }
    return writer;
  }

  @Override
  public boolean validateConnection() {
    // in bypass mode nothing is written, so the output file is never opened
    if (bypass) {
      return true;
    }
    ICSVWriter writer = getOrOpenWriter();
    if (writer == null) {
      return false;
    }
    if (includeHeader) {
      writer.writeNext(columns.toArray(new String[columns.size()]), true);
    }
    return true;
  }

  @Override
  protected Set<Pair<Document, Exception>> sendToIndex(List<Document> documents) throws Exception {
    if (bypass) {
      return Set.of();
    }
    ICSVWriter writer = getOrOpenWriter();
    if (writer == null) {
      throw new IndexerException("CSVIndexer could not open its output file: " + path);
    }
    for (Document doc : documents) {
      writer.writeNext(getLine(doc), true);
    }
    writer.flushQuietly();

    return Set.of();
  }

  private String[] getLine(Document doc) {
    String[] line = new String[columns.size()];
    for (int i = 0; i < columns.size(); i++) {
      String field = columns.get(i);
      if (doc.isMultiValued(field)) {
        line[i] = doc.getStringList(field).toString();
      } else {
        line[i] = doc.getString(columns.get(i));
      }
    }
    return line;
  }

  @Override
  public void closeConnection() {
    if (writer != null) {
      try {
        writer.close();
      } catch (IOException e) {
        log.warn("Error occurred when closing csv indexer writer.", e);
      }
    }
  }

}
