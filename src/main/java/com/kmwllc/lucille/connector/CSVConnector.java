package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

public class CSVConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);

  private final String path;

  public CSVConnector(Config config) {
    super(config);
    this.path = config.getString("path");
  }

  @Override
  public void start(Publisher publisher) throws ConnectorException {

    try (CSVReader csvReader = new CSVReader(FileUtils.getReader(path))) {

      // Assume first line is header
      String[] header = csvReader.readNext();
      if (header == null || header.length == 0) {
        return;
      }

      String[] line;
      int lineNum = 0;
      while ((line = csvReader.readNext()) != null) {
        lineNum++;

        // skip blank lines and lines with no value in the first column
        if (line.length == 0 || (line.length == 1 && StringUtils.isBlank(line[0]))) {
          continue;
        }

        Document doc = new Document(createDocId(line[0]));
        doc.setField("source", path);

        int maxIndex = Math.min(header.length, line.length);
        for (int i = 1; i < maxIndex; i++) {
          if (line[i] != null) {
            doc.setField(header[i], line[i]);
            doc.setField("csvLineNumber", lineNum);
          }
        }
        log.info("submitting " + doc);
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
