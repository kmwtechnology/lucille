package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Document;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;

public class CSVConnector implements Connector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);

  private final String path;

  public CSVConnector(String path) {
    this.path = path;
  }

  public CSVConnector(Config config) {
    this.path = config.getString("path");
  }

  @Override
  public void start(Publisher publisher) {

    CSVReader csvReader = null;
    try {
      Reader reader;
      if (path.startsWith("classpath:")) {
        InputStream is = getClass().getClassLoader().getResourceAsStream(path.substring(path.indexOf(":")+1));
        InputStreamReader isReader = new InputStreamReader(is);
        reader = new BufferedReader(isReader);
      } else {
        reader = Files.newBufferedReader(Path.of(path));
      }
      csvReader = new CSVReader(reader);

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

        String docId = line[0];
        Document doc = new Document(docId);
        doc.setField("source", path);

        int maxIndex = Math.min(header.length, line.length);
        for (int i = 0; i < maxIndex; i++) {
          if (line[i] != null) {
            doc.setField(header[i], line[i]);
            doc.setField("csvLineNumber", lineNum);
          }
        }
        log.info("submitting " + doc);
        publisher.publish(doc);

      }
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      try {
        if (csvReader!=null) {
          csvReader.close();
        }
      } catch (IOException ioe) {
        log.error("Error when closing CSV Reader.", ioe);
      }
    }

    log.info("produced " + publisher.numPublished() + " docs; complete");
  }

  public String toString() {
    return "CSVConnector: " + path;
  }
}
