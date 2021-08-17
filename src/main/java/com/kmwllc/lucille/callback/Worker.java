package com.kmwllc.lucille.callback;

import com.kmwllc.lucille.core.ConfigAccessor;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Pipeline;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

class Worker implements Runnable {

  private static final Logger log = LoggerFactory.getLogger(Worker.class);
  
  private final WorkerDocumentManager manager;

  private final Pipeline pipeline;

  private volatile boolean running = true;

  public void terminate() {
    log.info("terminate");
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
        log.info("polling");
        doc = manager.retrieveForProcessing();
      } catch (Exception e) {
        log.info("interrupted " + e);
        terminate();

        return;
      }

      if (doc == null) {
        log.info("WORKER: received 0 docs");
        continue;
      }

      try {
        List<Document> results = pipeline.processDocument(doc);

        for (Document result : results) {
          log.info("processed " + result);
          manager.submitCompleted(result);

          // create an open receipt for child documents
          String runId = doc.getString("run_id");
          if (!doc.getId().equals(result.getId())) {
            manager.submitConfirmation(new Event(result.getId(), runId, null, Event.Type.CREATE));
          }

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
    log.info("exit");
  }

}
