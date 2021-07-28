package com.kmwllc.lucille.scrap.callback;

import com.kmwllc.lucille.core.Document;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SimpleDocumentManager implements DocumentManager {

  private LinkedBlockingQueue<Document> docsToProcess = new LinkedBlockingQueue<>();
  private LinkedBlockingQueue<Document> docsToSend = new LinkedBlockingQueue<>();
  private List<Receipt> openReceipts = Collections.synchronizedList(new ArrayList<Receipt>());
  private List<Receipt> closedReceipts = Collections.synchronizedList(new ArrayList<Receipt>());
  private SolrClient client;

  private static final String SOLR_URL = "http://localhost:8983/solr/callbacktest";
  //private static final String COLLECTION = "callbacktest";

  public SimpleDocumentManager() {
    this.client = new HttpSolrClient.Builder(SOLR_URL).build();
  }

  public void submitForProcessing(Document document) {
    try {
      docsToProcess.put(document); // blocking
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public Document retrieveForProcessing() throws InterruptedException {
      return docsToProcess.take(); // blocking
  }

  public void submitCompleted(Document document) {
      docsToSend.add(document); // non blocking
  };

  public Document retrieveCompleted() throws InterruptedException {
    return docsToSend.take();
  };

  public List<Document> retrieveCompleted(int batchSize, int docTimeout, int batchTimeout) throws InterruptedException{

    List<Document> docs = new ArrayList();
    StopWatch watch = new StopWatch();
    watch.start();
    for (int i=0; i<batchSize; i++) {
      Document doc = docsToSend.poll(docTimeout, TimeUnit.MILLISECONDS);
      if (doc!=null) {
        docs.add(doc);
      }
      if (watch.getTime(TimeUnit.MILLISECONDS)>batchTimeout) {
        break;
      }
    }

    return docs;
  };


  public void sendToSolr(List<Document> documents) throws IOException, SolrServerException {
    List<SolrInputDocument> solrDocs = new ArrayList();
    for (Document doc : documents) {
      Map<String,Object> map = doc.asMap();
      SolrInputDocument solrDoc = new SolrInputDocument();
      for (String key : map.keySet()) {
        Object value = map.get(key);
        solrDoc.setField(key,value);
      }
      solrDocs.add(solrDoc);
    }
    client.add(solrDocs);
  }

  public void openReceipt(Receipt receipt) {
    openReceipts.add(receipt);
  }

  public void closeReceipt(Receipt receipt) {
    openReceipts.remove(receipt);
    closedReceipts.add(receipt);
  }

  public int countPending() {
    return openReceipts.size();
  }

  public int countClosed() {
    return closedReceipts.size();
  }

}
