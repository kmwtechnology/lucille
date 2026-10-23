package com.kmwllc.lucille.opensearch.provider;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;
import com.kmwllc.lucille.opensearch.indexer.OpenSearchIndexer;

/**
 * Registers {@link OpenSearchIndexer} for selection via {@code indexer.type: "OpenSearch"}.
 *
 * <p>Discovered by {@link com.kmwllc.lucille.indexer.IndexerFactory} through
 * {@code META-INF/services/com.kmwllc.lucille.indexer.IndexerTypeProvider}, which is how the OpenSearch backend keeps the
 * {@code indexer.type} shortcut working now that it lives outside lucille-core.
 */
public class OpenSearchIndexerProvider implements IndexerTypeProvider {

  @Override
  public Class<? extends Indexer> getIndexerClass() {
    return OpenSearchIndexer.class;
  }
}
