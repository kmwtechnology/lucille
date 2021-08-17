package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class CSVConnector implements Connector {

  private static final Logger log = LoggerFactory.getLogger(CSVConnector.class);

  private final String runId;

  private final String fileName;

  public CSVConnector(String runId, String fileName) {
    this.runId = runId;
    this.fileName = fileName;
  }

  @Override
  public void connect(List<String> documentIds, ConnectorDocumentManager manager) {

    try (Reader fileReader = Files.newBufferedReader(Path.of(fileName));
         CSVReader csvReader = new CSVReader(fileReader)) {

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
        // TODO: consider moving the setting of run_id to the ConnectorDocumentManager so individual
        // connector implementations don't have to worry about it
        doc.setField("run_id", runId);
        doc.setField("source", fileName);

        int maxIndex = Math.min(header.length, line.length);
        for (int i = 0; i < maxIndex; i++) {
          if (line[i] != null) {
            doc.setField(header[i], line[i]);
            doc.setField("csvLineNumber", lineNum);
          }
        }
        log.info("submitting " + doc);
        manager.submitForProcessing(doc);
        documentIds.add(docId);


/*
        // for testing, simulate latency
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
 */

      }
    } catch (Exception e) {
      e.printStackTrace();
    }

    log.info("produced " + documentIds.size() + " docs; complete");
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
