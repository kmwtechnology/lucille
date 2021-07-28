package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Pipeline;

import java.util.List;

class Worker implements Runnable {

  private final WorkerDocumentManager manager;

  private final Pipeline pipeline;

  private volatile boolean running = true;

  public void terminate() {
    Connector.log.info("WORKER: terminate");
    running = false;
  }

  public Worker() throws Exception {
    this.manager = new WorkerDocumentManager();
    this.pipeline = Pipeline.fromConfig(ConfigAccessor.loadConfig());
  }

  @Override
  public void run() {
    while (running) {
      Document doc;
      try {
        Connector.log.info("WORKER: polling");
        doc = manager.retrieveForProcessing();
      } catch (Exception e) {
        Connector.log.info("WORKER: interrupted " + e);
        terminate();

        return;
      }

      if (doc == null) {
        Connector.log.info("WORKER: received 0 docs");
        continue;
      }

      try {
        List<Document> results = pipeline.processDocument(doc);

        for (Document result : results) {
          Connector.log.info("WORKER: processed " + result);
          manager.submitCompleted(result);
        }

      } catch (Exception e) {

        /*
        try {
          manager.submitReceipt(new Receipt(doc.getId(), runId, e.getMessage()));
        } catch (Exception e2) {
          e2.printStackTrace();
        }
        */

        e.printStackTrace();
      }
    }
    try {
      manager.close();
    } catch (Exception e) {
      e.printStackTrace();
    }
    Connector.log.info("WORKER: exit");
  }

}
