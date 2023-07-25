package com.kmwllc.lucille.core;

import org.junit.Test;

import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SingleBatchTest {

  /**
   * Test adding a single document to a batch and retrieving it by flushing the whole batch.
   */
  @Test
  public void testSimpleAdd() {
    SingleBatch batch = new SingleBatch(100, 100);
    Document doc = Document.create("doc");
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
    SingleBatch batch = new SingleBatch(100, 1000);
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
   * Test that documents are returned if the batch is full.
   */
  @Test
  public void testFullBatch() {
    SingleBatch batch = new SingleBatch(1, 1000);
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
   * Test that supplying a null document to the batch causes a NullPointerException.
   */
  @Test(expected = NullPointerException.class)
  public void testSupplyNull() {
    SingleBatch batch = new SingleBatch(100, 1000);
    batch.add(null);
  }

  /**
   * Test that the batch is flushed if flushIfExpired() or add() is called after the time elapsed since the last
   *
   * @throws InterruptedException
   */
  @Test
  public void testTimeout() throws InterruptedException {
    SingleBatch batch = new SingleBatch(100, 10);
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
    SingleBatch batch = new SingleBatch(100, 100000);
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
