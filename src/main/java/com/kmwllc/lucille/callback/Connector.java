package com.kmwllc.lucille.callback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

public class Connector {

  private static final Logger log = LoggerFactory.getLogger(Connector.class);

  public static void main(String[] args) throws Exception {
    final String runId = UUID.randomUUID().toString();
    log.info("runId=" + runId);
    ConnectorDocumentManager manager = new ConnectorDocumentManager(runId);

    List<Receipt> receipts = Collections.synchronizedList(new ArrayList<Receipt>());
    List<Receipt> prematureReceipts = Collections.synchronizedList(new ArrayList<Receipt>());
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

        if (receipt.isOpen()) {
          if (!prematureReceipts.remove(receipt)) {
            receipts.add(receipt);
          }
        } else {
          if (!receipts.remove(receipt)) {
            prematureReceipts.add(receipt);
          }
        }
      }

      if (receipts.isEmpty() && prematureReceipts.isEmpty() && manager.isReceiptTopicEmpty(runId) && !producerThread.isAlive()) {
        break;
      }

      log.info("waiting on " + receipts.size() + " open receipts; " + prematureReceipts.size() + " prematureReceipts");
      //Thread.sleep(500);
    }

    log.info("all receipts closed");

    indexer.terminate();
    worker.terminate();
    manager.close();
  }
}
