package com.kmwllc.lucille.indexer.provider;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.CSVIndexer;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;

/**
 * Registers {@link CSVIndexer} for selection via {@code indexer.type: "CSV"}.
 */
public class CSVIndexerProvider implements IndexerTypeProvider {

  @Override
  public Class<? extends Indexer> getIndexerClass() {
    return CSVIndexer.class;
  }
}
