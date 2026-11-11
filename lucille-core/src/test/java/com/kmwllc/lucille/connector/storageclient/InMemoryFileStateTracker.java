package com.kmwllc.lucille.connector.storageclient;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A FileStateTracker for tests, holding state in memory. Shows that a StorageClient can be tested with state without
 * a database or a FileConnector.
 */
public class InMemoryFileStateTracker implements FileStateTracker {

  private final Map<String, Instant> lastPublished = new HashMap<>();
  private final Set<String> encountered = new HashSet<>();

  @Override
  public void markFileEncountered(String fullPathStr) {
    encountered.add(fullPathStr);
  }

  @Override
  public void markAllEntriesEncountered(String prefix) {
    lastPublished.keySet().stream().filter(key -> key.startsWith(prefix)).forEach(encountered::add);
  }

  @Override
  public Instant getLastPublished(String fullPathStr) {
    return lastPublished.get(fullPathStr);
  }

  @Override
  public void successfullyPublishedFile(String fullPathStr) {
    lastPublished.put(fullPathStr, Instant.now());
  }

  public Set<String> getEncountered() {
    return encountered;
  }

  public Map<String, Instant> getLastPublishedMap() {
    return lastPublished;
  }

  /** Clears the encountered files, as at the start of a new run. Keeps the publish times. */
  public void startNewRun() {
    encountered.clear();
  }
}
