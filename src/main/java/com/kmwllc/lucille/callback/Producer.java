package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;
import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;

import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

class Producer implements Runnable {

  private final List<Receipt> receipts;

  private final ProducerDocumentManager manager;

  private final String runId;

  private final String fileName;

  public Producer(String runId, String fileName, List<Receipt> receipts) {
    this.runId = runId;
    this.fileName = fileName;
    this.manager = new ProducerDocumentManager();
    this.receipts = receipts;
  }


  public List<Receipt> getReceipts() {
    return receipts;
  }

  @Override
  public void run() {

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
        doc.setField("run_id", runId);
        doc.setField("source", fileName);

        int maxIndex = Math.min(header.length, line.length);
        for (int i = 0; i < maxIndex; i++) {
          if (line[i] != null) {
            doc.setField(header[i], line[i]);
            doc.setField("csvLineNumber", lineNum);
          }
        }
        Connector.log.info("PRODUCER: submitting " + doc);
        manager.submitForProcessing(doc);
        receipts.add(new Receipt(docId, runId, null));


        // for testing, simulate latency
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          return;
        }
      }

    } catch (Exception e) {
      e.printStackTrace();
    }

    Connector.log.info("PRODUCER: produced " + receipts.size() + " docs; complete");
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}
