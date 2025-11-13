package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Indexer;
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
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>path (String, Required) : Output CSV file path.</li>
 *   <li>columns (List&lt;String&gt;, Required) : Ordered list of document fields to write as CSV columns.</li>
 *   <li>includeHeader (Boolean, Optional) : Write a header row with column names on connect. Defaults to true.</li>
 *   <li>append (Boolean, Optional) : Open the CSV in append mode instead of overwriting. Defaults to false.</li>
 * </ul>
 */
public class CSVIndexer extends Indexer {

  public static final Spec SPEC = SpecBuilder.indexer()
      .requiredString("path")
      .requiredList("columns", new TypeReference<List<String>>(){})
      .optionalBoolean("includeHeader", "append").build();

  private static final Logger log = LoggerFactory.getLogger(CSVIndexer.class);

  private final boolean bypass;
  private final ICSVWriter writer;
  private final List<String> columns;
  private final boolean includeHeader;

  /**
   * Creates a CSVIndexer from the given arguments.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   * @param localRunId The runID for a local run, null otherwise.
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, ICSVWriter writer, boolean bypass, String metricsPrefix, String localRunId) {
    super(config, messenger, metricsPrefix, localRunId);
    if (this.indexOverrideField != null) {
      throw new IllegalArgumentException(
          "Cannot create CSVIndexer. Config setting 'indexer.indexOverrideField' is not supported by CSVIndexer.");
    }
    this.writer = writer;
    this.bypass = bypass;
    this.columns = config.getStringList("csv.columns");
    this.includeHeader = config.hasPath("csv.includeHeader") ? config.getBoolean("csv.includeHeader") : true;
  }

  /**
   * Creates a CSVIndexer from the given arguments.
   * @param config Configuration for Lucille which should potentially contain "indexer" as well as "csv"
   *              (Configuration for the CSVIndexer)
   * @param localRunId The runID for a local run, null otherwise.
   */
  public CSVIndexer(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) {
    this(config, messenger, getCsvWriter(config, bypass), bypass, metricsPrefix, localRunId);
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
    this(config, messenger, getCsvWriter(config, bypass), bypass, metricsPrefix, null);
  }

  @Override
  protected String getIndexerConfigKey() { return "csv"; }

  private static ICSVWriter getCsvWriter(Config config, boolean bypass) {
    boolean append = config.hasPath("csv.append") ? config.getBoolean("csv.append") : false;
    try {
      File file = new File(config.getString("csv.path")).getAbsoluteFile();
      if (!file.getParentFile().exists()) {
        file.getParentFile().mkdirs();
      }
      CSVWriterBuilder builder = new CSVWriterBuilder(new FileWriter(file, append));
      return builder.build();
    } catch (IOException e) {
      log.error("Error initializing writer for CSVIndexer", e);
      return null;
    }
  }

  @Override
  public boolean validateConnection() {
    if (!bypass && writer == null) {
      return false;
    }
    if (includeHeader) {
      writer.writeNext(columns.toArray(new String[columns.size()]), true);
    }
    return true;
  }

  @Override
  protected Set<Pair<Document, String>> sendToIndex(List<Document> documents) throws Exception {
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
