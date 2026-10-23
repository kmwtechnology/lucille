package com.kmwllc.lucille.indexer.provider;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;
import com.kmwllc.lucille.indexer.NopIndexer;

/**
 * Registers {@link NopIndexer} for selection via {@code indexer.type: "Nop"}.
 */
public class NopIndexerProvider implements IndexerTypeProvider {

  @Override
  public Class<? extends Indexer> getIndexerClass() {
    return NopIndexer.class;
  }
}
