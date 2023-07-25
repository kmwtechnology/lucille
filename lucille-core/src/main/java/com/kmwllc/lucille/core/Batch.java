package com.kmwllc.lucille.core;

import java.util.List;

/**
 * Represents a reusable "batch" that can be filled with Documents.
 *
 * <p>Once the batch has reached a configured capacity, the next call to add() will flush the batch
 * and return all the Documents it contained. The new Document will not be included in the return value but will be stored as the
 * new first element of the batch.
 *
 * <p>A batch is considered to be "expired" if a configured timeout has elapsed since last add or
 * flush. If a batch is expired, it will be flushed during the next call to add() or flushIfExpired().
 */
public interface Batch {

  /**
   * Adds a Document to the Batch. If the batch has reached its capacity or if it is expired, it will be flushed and all of its
   * contents will be returned. The newly added document will not be returned but will be stored as the first element of the current
   * batch.
   */
  public List<Document> add(Document doc);

  /**
   * Removes and returns all Documents in the current batch if the batch is expired (i.e. if the configured timeout has been reached
   * since the last add or flush). Returns an empty list otherwise.
   */
  public List<Document> flushIfExpired();

  /**
   * Removes and returns all Documents in the current batch, regardless of whether the batch is expired.
   */
  public List<Document> flush();
}
