package com.kmwllc.lucille.connector.storageclient;

import java.io.IOException;
import java.net.URI;

/**
 * <p> Holds / manages a connection to a JDBC database used to maintain state regarding StorageClient traversals and the last
 * time files were processed and published by Lucille.
 * <p> The database can either be embedded in the JVM (H2 database), or you can build a connection to any JDBC-compatible
 * database which holds your state information.
 * <p> Call the init() and shutdown() methods to connect / disconnect to the database.
 * <p> Call the getStateEntriesFor(URI) method to build the appropriate / relevant state information for your traversal.
 * <p> Call the updateState(StorageClientState) method to update the database based on the results of your traversal.
 */
public class StorageClientStateManager {

  public void init() throws IOException {

  }

  public void shutdown() throws IOException {

  }

  public StorageClientState getStateForTraversal(URI startingDirectory) {
    return null;
  }

  public void updateState(StorageClientState state) {

  }
}
