package com.kmwllc.lucille.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Connector {

  public static final Logger log = LoggerFactory.getLogger(Connector.class);

  public static void main(String[] args) throws Exception {
    final String runId = UUID.randomUUID().toString();
    log.info("MAIN: runId=" + runId);
    ConnectorDocumentManager manager = new ConnectorDocumentManager(runId);

    List<Receipt> receipts = Collections.synchronizedList(new ArrayList<Receipt>());
    Producer producer = new Producer(runId, "/Volumes/Work/lucille/test.csv", receipts);
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
      Receipt receipt = manager.retrieveReceipt();

      if (receipt!=null) {
        log.info("RETRIEVED RECEIPT: " + receipt);
        receipts.remove(receipt);
      }

      if (receipts.isEmpty() && manager.isReceiptTopicEmpty(runId) && !producerThread.isAlive()) {
        break;
      }

      log.info("MAIN: waiting on " + receipts.size() + " open receipts");
      //Thread.sleep(500);
    }

    log.info("MAIN: all receipts closed");

    indexer.terminate();
    worker.terminate();
    manager.close();
  }
}
