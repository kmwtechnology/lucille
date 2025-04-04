package com.kmwllc.lucille.core;

import java.util.List;

/**
 * Represents a reusable "batch" that can be filled with Documents.
 *
 * Once the batch has reached a configured capacity, the next call
 * to add() will flush the batch and return all the Documents it contained.
 * The new Document will not be included in the return value but will
 * be stored as the new first element of the batch.
 *
 * A batch is considered to be "expired" if a configured timeout has
 * elapsed since last add or flush. If a batch is expired,
 * it will be flushed during the next call to add() or flushIfExpired().
 */
public interface Batch {

  /**
   * Adds a Document to the Batch.
   * If the batch has reached its capacity or if it is expired, it will be flushed
   * and all of its contents will be returned. The newly added document will not
   * be returned but will be stored as the first element of the current batch.
   *
   * @param doc The doc to add to the batch.
   * @return A list of documents that were flushed, if any. Will not contain the provided document.
   */
  public List<Document> add(Document doc);

  /**
   * Removes and returns all Documents in the current batch if the batch is expired
   * (i.e. if the configured timeout has been reached since the last add or flush).
   * Returns an empty list otherwise.
   *
   * @return If the batch is expired, all of its documents. If not, an empty list.
   */
  public List<Document> flushIfExpired();

  /**
   * Removes and returns all Documents in the current batch,
   * regardless of whether the batch is expired.
   *
   * @return All documents in the current batch.
   */
  public List<Document> flush();

  /**
   * Retrieves the capacity of a batch.
   */
  public int getCapacity();
}
