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
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE,100);
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
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE, 1000);
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
    SingleBatch batch = new SingleBatch(1, Long.MAX_VALUE, 1000);
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
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE, 1000);
    batch.add(null);
  }

  /**
   * Test that the batch is flushed if flushIfExpired() or add() is called after the time elapsed since the last
   *
   * @throws InterruptedException
   */
  @Test
  public void testTimeout() throws InterruptedException {
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE, 10);
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
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE, 100000);
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
    SingleBatch batch = new SingleBatch(100, Long.MAX_VALUE, 1000);
    assertEquals(100, batch.getCapacity());
  }

  /**
   * Test that the batch flushes based on accumulated byte size when the count cap is disabled.
   * With the byte cap set to the combined size of the first two docs, adding a third doc pushes the
   * accumulator past the cap and flushes the first two; the third doc carries into the next batch.
   */
  @Test
  public void testByteCapFlush() {
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    long byteCap = doc1.getByteSize() + doc2.getByteSize();
    SingleBatch batch = new SingleBatch(Indexer.NO_BATCH_SIZE, byteCap, 100000);

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.add(doc2).isEmpty());

    List<Document> docs = batch.add(doc3);
    assertEquals(2, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("doc2", docs.get(1).getId());

    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc3", docs.get(0).getId());
  }

  @Test
  public void testByteAccumulatorResetOnFlush() {
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");
    Document doc4 = Document.create("doc4");

    long byteCap = doc1.getByteSize() + doc2.getByteSize() + doc3.getByteSize();
    SingleBatch batch = new SingleBatch(Indexer.NO_BATCH_SIZE, byteCap, 100000);

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.add(doc2).isEmpty());

    List<Document> flushed = batch.flush();
    assertEquals(2, flushed.size());

    // two fresh docs fit under the cap and must not flush now that the accumulator is reset
    assertTrue(batch.add(doc3).isEmpty());
    assertTrue(batch.add(doc4).isEmpty());

    List<Document> docs = batch.flush();
    assertEquals(2, docs.size());
    assertEquals("doc3", docs.get(0).getId());
    assertEquals("doc4", docs.get(1).getId());
  }

  /**
   * Test that when a single document is larger than the byte cap, each document is emitted on its own.
   * The over-sized doc is retained and flushed by itself on the following add.
   */
  @Test
  public void testSingleDocExceedsByteCap() {
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    // a 1-byte cap is smaller than any document
    SingleBatch batch = new SingleBatch(Indexer.NO_BATCH_SIZE, 1, 100000);

    assertTrue(batch.add(doc1).isEmpty());

    List<Document> docs = batch.add(doc2);
    assertEquals(1, docs.size());
    assertEquals("doc1", docs.get(0).getId());

    docs = batch.add(doc3);
    assertEquals(1, docs.size());
    assertEquals("doc2", docs.get(0).getId());

    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc3", docs.get(0).getId());
  }

  /**
   * Test that the byte cap flushes repeatedly across many adds, each flush staying within the cap.
   * With equal-sized docs and a cap of two docs, every third add triggers a flush of two.
   */
  @Test
  public void testByteCapMultipleFlushes() {
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");
    Document doc4 = Document.create("doc4");
    Document doc5 = Document.create("doc5");
    Document doc6 = Document.create("doc6");

    // ids are the same length, so all docs serialize to the same byte size
    long byteCap = doc1.getByteSize() + doc2.getByteSize();
    SingleBatch batch = new SingleBatch(Indexer.NO_BATCH_SIZE, byteCap, 100000);

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.add(doc2).isEmpty());

    List<Document> firstFlush = batch.add(doc3);
    assertEquals(2, firstFlush.size());
    assertEquals("doc1", firstFlush.get(0).getId());
    assertEquals("doc2", firstFlush.get(1).getId());

    assertTrue(batch.add(doc4).isEmpty());

    List<Document> secondFlush = batch.add(doc5);
    assertEquals(2, secondFlush.size());
    assertEquals("doc3", secondFlush.get(0).getId());
    assertEquals("doc4", secondFlush.get(1).getId());

    assertTrue(batch.add(doc6).isEmpty());

    List<Document> finalFlush = batch.flush();
    assertEquals(2, finalFlush.size());
    assertEquals("doc5", finalFlush.get(0).getId());
    assertEquals("doc6", finalFlush.get(1).getId());
  }

  /**
   * Test that when both caps are set, the byte cap can be the limiting factor.
   * Here the count cap is generous, so the batch flushes once the accumulated bytes are exceeded.
   */
  @Test
  public void testByteCapBeatsCountCap() {
    Document doc1 = Document.create("doc1");
    Document doc2 = Document.create("doc2");
    Document doc3 = Document.create("doc3");

    // count cap large enough to never trip; byte cap of two docs governs
    long byteCap = doc1.getByteSize() + doc2.getByteSize();
    SingleBatch batch = new SingleBatch(100, byteCap, 100000);

    assertTrue(batch.add(doc1).isEmpty());
    assertTrue(batch.add(doc2).isEmpty());

    List<Document> docs = batch.add(doc3);
    assertEquals(2, docs.size());
    assertEquals("doc1", docs.get(0).getId());
    assertEquals("doc2", docs.get(1).getId());

    docs = batch.flush();
    assertEquals(1, docs.size());
    assertEquals("doc3", docs.get(0).getId());
  }

  /**
   * Test that when both caps are set, each one can be the trigger on different flushes within a single run.
   * A run of small docs flushes on the count cap, then a run of large docs flushes on the byte cap.
   */
  @Test
  public void testBothCapsFlexOverMultipleFlushes() {
    Document small1 = Document.create("s1");
    Document small2 = Document.create("s2");
    Document small3 = Document.create("s3");
    Document small4 = Document.create("s4");
    Document big1 = bigDoc("b1");
    Document big2 = bigDoc("b2");

    long smallSize = small1.getByteSize();
    long bigSize = big1.getByteSize();
    int countCap = 3;
    long byteCap = 2 * bigSize;

    // preconditions that make this scenario deterministic
    assertTrue("small docs must not trip the byte cap on the count flush", countCap * smallSize <= byteCap);
    assertTrue("one small + one big must fit under the byte cap", smallSize + bigSize <= byteCap);

    SingleBatch batch = new SingleBatch(countCap, byteCap, 100000);

    assertTrue(batch.add(small1).isEmpty());
    assertTrue(batch.add(small2).isEmpty());
    assertTrue(batch.add(small3).isEmpty());

    // count cap (3) is exceeded by the 4th add -> flush the three small docs
    List<Document> countFlush = batch.add(small4);
    assertEquals(3, countFlush.size());
    assertEquals("s1", countFlush.get(0).getId());
    assertEquals("s2", countFlush.get(1).getId());
    assertEquals("s3", countFlush.get(2).getId());

    assertTrue(batch.add(big1).isEmpty());

    // byte cap is exceeded by big2 (s4 + big1 + big2 > 2 * bigSize) -> byte flush
    List<Document> byteFlush = batch.add(big2);
    assertEquals(2, byteFlush.size());
    assertEquals("s4", byteFlush.get(0).getId());
    assertEquals("b1", byteFlush.get(1).getId());

    List<Document> finalFlush = batch.flush();
    assertEquals(1, finalFlush.size());
    assertEquals("b2", finalFlush.get(0).getId());
  }

  private static Document bigDoc(String id) {
    Document doc = Document.create(id);
    doc.setField("payload", "x".repeat(500));
    return doc;
  }

}
