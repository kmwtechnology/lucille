package com.kmwllc.lucille.elasticsearch.provider;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.elasticsearch.indexer.ElasticsearchIndexer;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;

/**
 * Registers {@link ElasticsearchIndexer} for selection via {@code indexer.type: "Elasticsearch"}.
 *
 * <p>Discovered by {@link com.kmwllc.lucille.indexer.IndexerFactory} through
 * {@code META-INF/services/com.kmwllc.lucille.indexer.IndexerTypeProvider}, which is how the Elasticsearch backend keeps
 * the {@code indexer.type} shortcut working now that it lives outside lucille-core.
 */
public class ElasticsearchIndexerProvider implements IndexerTypeProvider {

  @Override
  public Class<? extends Indexer> getIndexerClass() {
    return ElasticsearchIndexer.class;
  }
}
