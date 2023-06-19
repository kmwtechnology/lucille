package com.kmwllc.lucille.core;

import org.junit.Test;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultiBatchTest {
  /**
   * Test adding a single document to a batch and retrieving it by flushing the whole batch.
   */
  @Test
  public void testSimpleAdd() {
    MultiBatch batch = new MultiBatch(100, 100, "index");
    Document doc = Document.create("doc");
    List<Document> docs = batch.add(doc);
    assertTrue(docs.isEmpty());
    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc", docs.get(0).getId());
  }

  /**
   * Test adding a single document targetted to a specific index to a batch and retrieving it by flushing the whole batch.
   */
  @Test
  public void testSimpleAddToIndex() {
    MultiBatch batch = new MultiBatch(100, 100, "index");
    Document doc = Document.create("doc");
    doc.setField("index", "index1");
    List<Document> docs = batch.add(doc);
    assertTrue(docs.isEmpty());
    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc", docs.get(0).getId());
  }

  /**
   * Test adding a several documents to a batch and retrieving them by flushing the whole batch.
   */
  @Test
  public void testSeveralAdd() {
    MultiBatch batch = new MultiBatch(100, 1000, "index");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

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

  /**
   * Test adding a several documents to a batch targetting separate indices and retrieving them by flushing the whole batch.
   */
  @Test
  public void testSeveralAddToIndices() {
    MultiBatch batch = new MultiBatch(100, 1000, "index");
    Document doc1 = Document.create("doc1");
    doc1.setField("index", "index1");
    Document doc2 = Document.create("doc2");
    doc2.setField("index", "index2");
    Document doc3 = Document.create("doc3");
    doc3.setField("index", "index3");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc3);
    assertTrue(docs.isEmpty());

    docs = batch.flush();
    docs.sort(Comparator.comparing(Document::getId));
    assertEquals(3, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("index1", docs.get(0).getString("index"));
    assertEquals("doc2", docs.get(1).getId());
    assertEquals("index2", docs.get(1).getString("index"));
    assertEquals("doc3", docs.get(2).getId());
    assertEquals("index3", docs.get(2).getString("index"));
  }

  /**
   * Test that documents are returned if the batch is full.
   */
  @Test
  public void testFullBatch() {
    MultiBatch batch = new MultiBatch(1, 1000, "index");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc2", docs.get(0).getId());
  }
  /**
   * Test that documents are returned if one of the batches is full. Only the full batch should return docs.
   */
  @Test
  public void testOneBatchisFull() {
    MultiBatch batch = new MultiBatch(1, 1000, "index");
    Document doc1 = Document.create("doc1");
    doc1.setField("index", "index1");
    Document doc2 = Document.create("doc2");
    doc2.setField("index", "index2");
    Document doc3 = Document.create("doc3");
    doc3.setField("index", "index3");
    Document doc4 = Document.create("doc4");
    doc4.setField("index", "index2");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc2);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc3);
    assertTrue(docs.isEmpty());
    docs = batch.add(doc4);
    assertEquals(1, docs.size());
    assertEquals("doc2", docs.get(0).getId());
    docs = batch.flush();
    assertEquals(3, docs.size());
    docs.sort(Comparator.comparing(Document::getId));
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("doc3", docs.get(1).getId());
    assertEquals("doc4", docs.get(2).getId());
  }

  /**
   * Test that supplying a null document to the batch causes a NullPointerException.
   */
  @Test(expected = NullPointerException.class)
  public void testSupplyNull() {
    MultiBatch batch = new MultiBatch(100, 1000, "index");
    batch.add(null);
  }

  /**
   * Test that the batch is flushed if flushIfExpired() or add() is called after the time elapsed since the last
   *
   * @throws InterruptedException
   */
  @Test
  public void testTimeout() throws InterruptedException {
    MultiBatch batch = new MultiBatch(100, 10, "index");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    List<Document> docs = batch.add(doc1);
    assertTrue(docs.isEmpty());
    TimeUnit.MILLISECONDS.sleep(15);
    docs = batch.flushIfExpired();
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


  /**
   * Test that the batch is NOT flushed if flushIfExpired() or add() is called before the timeout.
   */
  @Test
  public void testNonTimeout() throws InterruptedException {
    MultiBatch batch = new MultiBatch(100, 100000, "index");
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.flushIfExpired().isEmpty());

    assertTrue(batch.add(doc2).isEmpty());
    assertTrue(batch.flushIfExpired().isEmpty());

    assertTrue(batch.add(doc3).isEmpty());
    assertTrue(batch.flushIfExpired().isEmpty());

    assertEquals(3, batch.flush().size());
  }
}
