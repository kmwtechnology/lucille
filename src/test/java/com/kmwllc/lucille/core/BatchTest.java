package com.kmwllc.lucille.core;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class BatchTest {

  @Test
  public void testSimpleAdd() {
    Batch batch = new Batch(100, 100);
    Document doc = new Document("doc");
    List<Document> docs = batch.add(doc);
    assertTrue(docs.isEmpty());
    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc", docs.get(0).getId());
  }

  @Test
  public void testSeveralAdd() {
    Batch batch = new Batch(100, 1000);
    Document doc1 = new Document("doc1");
    Document doc2 = new Document("doc2");
    Document doc3 = new Document("doc3");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc3);
    assertTrue(docs.isEmpty());

    docs = batch.flush();
    assertEquals(3, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("doc2", docs.get(1).getId());
    assertEquals("doc3", docs.get(2).getId());
  }

  @Test
  public void testFullBatch() {
    Batch batch = new Batch(1, 1000);
    Document doc1 = new Document("doc1");
    Document doc2 = new Document("doc2");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc2", docs.get(0).getId());
  }

  @Test
  public void testSupplyNull() {
    Batch batch = new Batch(100, 1000);
    Document doc1 = new Document("doc1");
    Document doc2 = new Document("doc2");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(null);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertTrue(docs.isEmpty());
  }

  @Test
  public void testTimeout() throws InterruptedException {
    Batch batch = new Batch(100, 10);
    Document doc1 = new Document("doc1");
    Document doc2 = new Document("doc2");
    Document doc3 = new Document("doc3");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    TimeUnit.MILLISECONDS.sleep(15);
    docs = batch.add(null);
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertTrue(batch.flush().isEmpty());

    docs = batch.add(doc2);
    assertTrue(docs.isEmpty());
    TimeUnit.MILLISECONDS.sleep(15);
    docs = batch.add(doc3);
    assertEquals(1, docs.size());
    assertEquals("doc2", docs.get(0).getId());
    assertEquals(1, batch.flush().size());
  }

}
