package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;

/**
 * Lightweight {@link java.util.ServiceLoader} provider that exposes an {@link Indexer} implementation so it can be
 * selected by its {@code indexer.type} name (e.g. "Solr") without {@code lucille-core} referencing the
 * implementation class directly.
 *
 * <p>This exists because {@code ServiceLoader} requires every service implementation to have a public no-arg
 * constructor, and Indexer implementations only expose the {@code (Config, IndexerMessenger, boolean, String, String)}
 * constructor. Rather than add a misleading no-arg constructor to every Indexer, each module that ships an Indexer also
 * ships a trivial no-arg provider and declares it in
 * {@code META-INF/services/com.kmwllc.lucille.indexer.IndexerTypeProvider}.
 *
 * <p>The provider only identifies the Indexer class. The type name is read from the Indexer's own
 * {@code public static final String INDEXER_TYPE} field, and construction is performed centrally by
 * {@link IndexerFactory} via the standard Indexer constructor — so providers carry no construction logic and nothing is
 * duplicated.
 */
@FunctionalInterface
public interface IndexerTypeProvider {

  /**
   * The {@link Indexer} implementation this provider registers. The class must declare a
   * {@code public static final String INDEXER_TYPE} naming its {@code indexer.type}, and the standard
   * {@code (Config, IndexerMessenger, boolean, String, String)} constructor.
   *
   * @return the Indexer implementation class.
   */
  Class<? extends Indexer> getIndexerClass();
}
