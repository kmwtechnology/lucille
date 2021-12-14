package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerFactory {
  public static final String DEFAULT_INDEXER_TYPE = "Solr";
  private static final Logger log = LoggerFactory.getLogger(Indexer.class);

  /**
   * Instantiates an Indexer from the designated Config.
   */
  public static Indexer fromConfig(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) throws ClassNotFoundException, NoSuchMethodException,
      IllegalAccessException, IndexerException {
    String typeName;

    // fetch indexer type if specified
    if (config.hasPath("indexer.type")) {
      typeName = config.getString("indexer.type");
    } else {
      log.info("Config setting for indexer.type was not specified, using default indexer type of '" + DEFAULT_INDEXER_TYPE + "'.");
      typeName = DEFAULT_INDEXER_TYPE;
    }

    // handle type class instantiation or throw exception for unknown type
    if (typeName.equalsIgnoreCase("Solr")) {
      return new SolrIndexer(config, manager, bypass, metricsPrefix);
    } else if (typeName.equalsIgnoreCase("OpenSearch")) {
      return new OpenSearchIndexer(config, manager, bypass, metricsPrefix);
    } else {
      throw new IndexerException("Unknown indexer.type configuration of: '" + typeName + "'");
    }
  }
}
