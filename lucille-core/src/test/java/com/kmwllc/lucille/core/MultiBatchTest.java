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
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 100, "index");
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
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 100, "index");
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
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 1000, "index");
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
   * Test adding a several documents to a batch targeting separate indices and retrieving them by flushing the whole batch.
   */
  @Test
  public void testSeveralAddToIndices() {
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 1000, "index");
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
    MultiBatch batch = new MultiBatch(1, Long.MAX_VALUE, 1000, "index");
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
    MultiBatch batch = new MultiBatch(1, Long.MAX_VALUE, 1000, "index");
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
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 1000, "index");
    batch.add(null);
  }

  /**
   * Test that the batch is flushed if flushIfExpired() or add() is called after the time elapsed since the last
   *
   * @throws InterruptedException
   */
  @Test
  public void testTimeout() throws InterruptedException {
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 10, "index");
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
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 100000, "index");
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

  @Test
  public void testGetCapacity() throws InterruptedException {
    MultiBatch batch = new MultiBatch(100, Long.MAX_VALUE, 1000, "index");
    assertEquals(100, batch.getCapacity());
  }

  /**
   * Test that the byte cap is enforced per index, not globally across the MultiBatch.
   * Each index gets its own byte budget equal to two docs, so adding a third doc to a given index
   * flushes only that index's docs while the other index is untouched.
   */
  @Test
  public void testByteCapPerIndex() {
    Document a1 = Document.create("a1");
    a1.setField("index", "indexA");
    Document a2 = Document.create("a2");
    a2.setField("index", "indexA");
    Document a3 = Document.create("a3");
    a3.setField("index", "indexA");
    Document b1 = Document.create("b1");
    b1.setField("index", "indexB");
    Document b2 = Document.create("b2");
    b2.setField("index", "indexB");

    // ids are the same length, so all docs serialize to the same byte size
    long byteCap = a1.getByteSize() + a2.getByteSize();
    MultiBatch batch = new MultiBatch(Indexer.NO_BATCH_SIZE, byteCap, 100000, "index");

    assertTrue(batch.add(a1).isEmpty());
    assertTrue(batch.add(b1).isEmpty());
    assertTrue(batch.add(a2).isEmpty());
    assertTrue(batch.add(b2).isEmpty());

    // indexA's accumulated bytes exceed the cap on its third doc; indexB is unaffected
    List<Document> docs = batch.add(a3);
    assertEquals(2, docs.size());
    docs.sort(Comparator.comparing(Document::getId));
    assertEquals("a1", docs.get(0).getId());
    assertEquals("a2", docs.get(1).getId());

    docs = batch.flush();
    docs.sort(Comparator.comparing(Document::getId));
    assertEquals(3, docs.size());
    assertEquals("a3", docs.get(0).getId());
    assertEquals("b1", docs.get(1).getId());
    assertEquals("b2", docs.get(2).getId());
  }

  /**
   * Test that the byte cap flushes repeatedly for a single index across many adds.
   */
  @Test
  public void testByteCapMultipleFlushes() {
    MultiBatch batch;
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");
    Document doc4 = Document.create("doc4");

    long byteCap = doc1.getByteSize() + doc2.getByteSize();
    batch = new MultiBatch(Indexer.NO_BATCH_SIZE, byteCap, 100000, "index");

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.add(doc2).isEmpty());

    List<Document> firstFlush = batch.add(doc3);
    assertEquals(2, firstFlush.size());
    assertEquals("doc1", firstFlush.get(0).getId());
    assertEquals("doc2", firstFlush.get(1).getId());

    assertTrue(batch.add(doc4).isEmpty());

    List<Document> finalFlush = batch.flush();
    assertEquals(2, finalFlush.size());
    assertEquals("doc3", finalFlush.get(0).getId());
    assertEquals("doc4", finalFlush.get(1).getId());
  }
}
