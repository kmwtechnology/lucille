package com.kmwllc.lucille.connector.storageclient;

import java.io.IOException;

/**
 * A {@link FileStateTracker} that can be used from several threads during one traversal, as long as each thread opens
 * its own state first. A StorageClient traversing with more than one thread requires one of these, and calls
 * {@link #openForThread()} on each of its threads before that thread's first per-file call. A plain FileStateTracker
 * is only ever called from one thread, and is rejected when a traversal would use more.
 */
public interface ThreadBoundFileStateTracker extends FileStateTracker {

  /**
   * Prepares the calling thread to make per-file calls. Every call is paired with {@link #closeForThread()}.
   * @throws IOException If the thread's state can't be opened. The traversal fails rather than carrying on without
   * recording what it visits, which would make those files look deleted.
   */
  void openForThread() throws IOException;

  /**
   * Releases whatever {@link #openForThread()} opened for the calling thread. Does nothing if the thread has nothing open.
   */
  void closeForThread();
}
