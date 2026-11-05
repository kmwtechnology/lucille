package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

/**
 * A StorageClient for tests of the shared traversal, over the object keys in "keys" in its config. Lists them by object
 * store rules (a prefix and a '/' delimiter), and records what the traversal asks for. Registered for a URI scheme through
 * "storageClients". A key's content is the key itself, or "contents.&lt;key&gt;"'s value in {@link #contentFor}'s override.
 */
public class TreeStorageClient extends BaseStorageClient {

  final Queue<String> listedPrefixes = new ConcurrentLinkedQueue<>();
  final Set<Thread> listingThreads = ConcurrentHashMap.newKeySet();
  final Queue<String> moves = new ConcurrentLinkedQueue<>();
  private final List<String> keys;

  public TreeStorageClient(Config config) {
    super(config);
    this.keys = config.getStringList("keys");
  }

  // a test can override this to observe or interfere with a listing
  protected void beforeListing(String prefix) { }

  protected String contentFor(String key) {
    return key;
  }

  @Override
  protected void initializeStorageClient() { }

  @Override
  protected void shutdownStorageClient() { }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileStateTracker stateMgr) throws ConnectorException {
    traverseByListing(publisher, params, stateMgr);
  }

  @Override
  protected Stream<ListingEntry> listDirectory(String prefix, TraversalParams params) {
    listedPrefixes.add(prefix);
    listingThreads.add(Thread.currentThread());
    beforeListing(prefix);

    List<ListingEntry> entries = new ArrayList<>();
    Set<String> directories = new TreeSet<>();

    for (String key : keys) {
      if (key.startsWith(prefix)) {
        int slash = key.indexOf('/', prefix.length());
        if (slash >= 0) {
          directories.add(key.substring(0, slash + 1));
        } else {
          entries.add(ListingEntry.file(key, () -> new TreeFileReference(key, params)));
        }
      }
    }

    directories.forEach(directory -> entries.add(ListingEntry.directory(directory)));
    return entries.stream();
  }

  @Override
  protected Stream<ListingEntry> listFlat(String prefix, TraversalParams params) {
    listedPrefixes.add("flat:" + prefix);
    return keys.stream()
        .filter(key -> key.startsWith(prefix))
        .map(key -> ListingEntry.file(key, () -> new TreeFileReference(key, params)));
  }

  @Override
  protected URI directoryURI(String prefix, TraversalParams params) {
    return uriFor(prefix, params);
  }

  private static URI uriFor(String key, TraversalParams params) {
    URI base = params.getURI();
    try {
      return new URI(base.getScheme(), base.getAuthority(), "/" + key, null, null);
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) {
    return new ByteArrayInputStream(contentFor(keyPrefix(uri)).getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public void moveFile(URI filePath, URI folder) {
    moves.add(filePath.toString());
  }

  private class TreeFileReference extends BaseFileReference {

    private final String key;

    TreeFileReference(String key, TraversalParams params) {
      super(uriFor(key, params), Instant.parse("2026-01-01T00:00:00Z"), 1L, null);
      this.key = key;
    }

    @Override
    public String getName() {
      return lastSegment(key);
    }

    @Override
    public String getFilterName() {
      return key;
    }

    @Override
    public boolean isValidFile() {
      return true;
    }

    @Override
    public InputStream getContentStream(TraversalParams params) throws java.io.IOException {
      return getFileContentStream(getFullPath());
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) throws java.io.IOException {
      try (InputStream in = getContentStream(params)) {
        return in.readAllBytes();
      }
    }
  }
}
