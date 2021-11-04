package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVParser;
import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;

public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);

  private final String path;
  private final String lineNumField;
  private final String idField;
  private final char separatorChar;
  private final char quoteChar;

  public CSVConnector(Config config) {
    super(config);
    this.path = config.getString("path");
    this.lineNumField = config.hasPath("lineNumberField") ? config.getString("lineNumberField") : "csvLineNumber";
    this.idField = config.hasPath("idField") ? config.getString("idField") : null;
    this.separatorChar = (config.hasPath("useTabs") && config.getBoolean("useTabs")) ? '\t' : ',';
    this.quoteChar = (config.hasPath("interpretQuotes") && !config.getBoolean("interpretQuotes")) ?
      CSVParser.NULL_CHARACTER : CSVParser.DEFAULT_QUOTE_CHARACTER;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    Instant startDocSet = Instant.now();
    try (CSVReader csvReader = new CSVReaderBuilder(FileUtils.getReader(path)).
      withCSVParser(new CSVParserBuilder().withSeparator(separatorChar).withQuoteChar(quoteChar).build()).build()) {

      // Assume first line is header
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        return;
      }

      int idFieldNum = 0;
      if (idField != null) {
        for (int i = 0; i < header.length; i++) {
          if (idField.equals(header[i])) {
            idFieldNum = i;
            break;
          }
        }
      }

      String[] line;
      int lineNum = 0;
      while ((line = csvReader.readNext()) != null) {
        lineNum++;

        // skip blank lines, lines with no value in the first column
        if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
          continue;
        }

        if (line.length != header.length) {
          log.warn(String.format("Line %d of the csv has a different number of columns than columns in the header.", lineNum));
          continue;
        }

        Document doc = new Document(createDocId(line[idFieldNum]));
        doc.setField("source", path);

        int maxIndex = Math.min(header.length, line.length);
        for (int i = 0; i < maxIndex; i++) {
          if (line[i] != null && !Document.RESERVED_FIELDS.contains(header[i])) {
            doc.setField(header[i], line[i]);
            doc.setField(lineNumField, lineNum);
          }
        }
        // log.info("submitting " + doc);
        publisher.publish(doc);
      }
    } catch (Exception e) {
      log.error("Error during CSV processing", e);
    }

    log.info("produced " + publisher.numPublished() + " docs; complete");
  }

  public String toString() {
    return "CSVConnector: " + path;
  }
}
