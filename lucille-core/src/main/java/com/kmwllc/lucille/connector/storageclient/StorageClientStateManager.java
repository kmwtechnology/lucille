package com.kmwllc.lucille.connector.storageclient;

import java.io.IOException;
import java.net.URI;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding StorageClient traversals and the last
 * time files were processed and published by Lucille.
 * <p> The database can either be embedded in the JVM (H2 database), or you can build a connection to any JDBC-compatible
 * database which holds your state information.
 * <p> Call {@link #init()} and {@link #shutdown()} to connect / disconnect to the database.
 * <p> Call {@link #getStateForTraversal(URI)} to build the appropriate / relevant state information for your traversal. Invoke the
 * appropriate methods on the returned {@link StorageClientState} as you progress throughout your traversal. This object allows
 * you to lookup when a file was last known to be published by Lucille, based on the information in the state database.
 * <p> After your traversal completes, call {@link #updateState(StorageClientState)} method to update the database based on the
 * results of your traversal. This will update the database to reflect the results of your traversal - like which files were
 * successfully published, which files were not encountered (deleted), etc.
 *
 * <p> <b>Note:</b> This class is operating under two key assumptions about FileConnector / Connectors:
 * <p> 1. Connectors run sequentially.
 * <p> 2. FileConnector does not support individual multithreading.
 */
public class StorageClientStateManager {

  // Builds a connection to the database where state is stored - either embedded or a specified JDBC connection / config
  public void init() throws IOException {

  }

  // Closes the connection to the database where state is stored - either embedded or a specified JDBC connection / config
  public void shutdown() throws IOException {

  }

  // Gets State information relevant for a traversal which starts at the given directory.
  public StorageClientState getStateForTraversal(URI startingDirectory) {
    return null;
  }

  // Updates the database to reflect the given state, which should have been updated as files were encountered and published.
  public void updateState(StorageClientState state) {

  }
}
