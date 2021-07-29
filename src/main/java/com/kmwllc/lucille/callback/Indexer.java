package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.Document;

import java.util.Collections;

class Indexer implements Runnable {

  private final IndexerDocumentManager manager;

  private volatile boolean running = true;

  public void terminate() {
    running = false;
    Connector.log.info("INDEXER: terminate");
  }

  public Indexer() {
    this.manager = new IndexerDocumentManager();
  }

  @Override
  public void run() {
    while (running) {
      Document doc;
      try {
        Connector.log.info("INDEXER: polling");
        doc = manager.retrieveCompleted();
      } catch (Exception e) {
        Connector.log.info("Indexer interrupted ", e);
        terminate();
        return;
      }
      if (doc == null) {
        Connector.log.info("INDEXER: received nothing");
        continue;
      }

      // TODO
      if (!doc.has("run_id")) {
        continue;
      }

      String runId = doc.getString("run_id");
      try {
        manager.sendToSolr(Collections.singletonList(doc));
        Receipt receipt = new Receipt(doc.getId(), runId, "SUCCEEDED");
        Connector.log.info("INDEXER: submitting receipt " + receipt);
        manager.submitReceipt(receipt);
      } catch (Exception e) {
        try {
          manager.submitReceipt(new Receipt(doc.getId(), runId, "FAILED" + e.getMessage()));
        } catch (Exception e2) {
          e2.printStackTrace();
        }
      }
    }
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Connector.log.info("INDEXER: exit");
  }

}
