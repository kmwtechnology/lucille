package com.kmwllc.lucille.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Runner {

  private static final Logger log = LoggerFactory.getLogger(Runner.class);

  public static void main(String[] args) throws Exception {
    final String runId = UUID.randomUUID().toString();
    log.info("runId=" + runId);
    RunnerDocumentManager manager = new RunnerDocumentManager(runId);

    // TODO: consider changing these to ConcurrentHashSets
    List<Confirmation> expectedConfirmations = Collections.synchronizedList(new ArrayList<Confirmation>());
    List<Confirmation> earlyConfirmations = Collections.synchronizedList(new ArrayList<Confirmation>());
    Producer producer = new Producer(runId, "/Volumes/Work/lucille/test.csv", expectedConfirmations);
    Indexer indexer = new Indexer();
    Worker worker = new Worker();
    Thread producerThread = new Thread(producer);
    Thread workerThread = new Thread(worker);
    Thread indexerThread = new Thread(indexer);
    producerThread.start();
    indexerThread.start();
    workerThread.start();

//    producerThread.join();
//    List<Receipt> receipts = producer.getReceipts();

    while (true) {
      Confirmation confirmation = manager.retrieveConfirmation();

      if (confirmation !=null) {
        log.info("RETRIEVED CONFIRMATION: " + confirmation);

        if (confirmation.isExpected()) {
          if (!earlyConfirmations.remove(confirmation)) {
            expectedConfirmations.add(confirmation);
          }
        } else {
          if (!expectedConfirmations.remove(confirmation)) {
            earlyConfirmations.add(confirmation);
          }
        }
      }

      if (expectedConfirmations.isEmpty() && earlyConfirmations.isEmpty() && manager.isConfirmationTopicEmpty(runId) && !producerThread.isAlive()) {
        break;
      }

      log.info("waiting on " + expectedConfirmations.size() + " expected confirmations; " + earlyConfirmations.size() + " early confirmations");
      //Thread.sleep(500);
    }

    log.info("all receipts closed");

    indexer.terminate();
    worker.terminate();
    manager.close();
  }
}
