package com.kmwllc.lucille.solr.provider;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;
import com.kmwllc.lucille.solr.indexer.SolrIndexer;

/**
 * Registers {@link SolrIndexer} for selection via {@code indexer.type: "Solr"}.
 *
 * <p>Discovered by {@link com.kmwllc.lucille.indexer.IndexerFactory} through
 * {@code META-INF/services/com.kmwllc.lucille.indexer.IndexerTypeProvider}, which is how the Solr backend keeps the
 * {@code indexer.type} shortcut working now that it lives outside lucille-core.
 */
public class SolrIndexerProvider implements IndexerTypeProvider {

  @Override
  public Class<? extends Indexer> getIndexerClass() {
    return SolrIndexer.class;
  }
}
