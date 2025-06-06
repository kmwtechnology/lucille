package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class IndexerFactory {

  private static final Logger log = LoggerFactory.getLogger(IndexerFactory.class);

  /**
   * Instantiates an Indexer from the designated Config.
   */
  public static Indexer fromConfig(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId)
      throws IndexerException {

    if (bypass == false) {
      if (config.hasPath("indexer.sendEnabled") && !config.getBoolean("indexer.sendEnabled")) {
        log.warn("indexer.sendEnabled is set to false in the configuration; indexer will be started in bypass mode");
        bypass = true;
      }
    }

    // prioritize using class, when present. will check for type as a "fallback".
    if (config.hasPath("indexer.class")) {
      return indexerFromClass(config, messenger, bypass, metricsPrefix, localRunId);
    } else if (config.hasPath("indexer.type")) {
      return indexerFromType(config, messenger, bypass, metricsPrefix, localRunId);
    } else {
      throw new IndexerException("No indexer.class or indexer.type specified in Config.");
    }
  }

  /**
   * Instantiates an Indexer from the designated Config, without a localRunId.
   */
  public static Indexer fromConfig(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix)
      throws IndexerException {
    return fromConfig(config, messenger, bypass, metricsPrefix, null);
  }

  // Creates an indexer from the given config's "indexer.class". Throws an IndexerException if it cannot be created.
  private static Indexer indexerFromClass(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    String className = config.getString("indexer.class");

    try {
      Class<?> clazz = Class.forName(className);
      Constructor<?> constructor = clazz.getConstructor(Config.class, IndexerMessenger.class, Boolean.TYPE, String.class, String.class);
      return (Indexer) constructor.newInstance(config, messenger, bypass, metricsPrefix, localRunId);
    } catch (ReflectiveOperationException e) {
      throw new IndexerException("Problem initializing indexer.class configuration of: '" + className + "'", e);
    }
  }

  // Creates an indexer based on the config's "indexer.type". Throws an exception if it is not a supported type (solr, opensearch, elasticsearch, csv).
  private static Indexer indexerFromType(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    String typeName = config.getString("indexer.type");

    if (typeName.equalsIgnoreCase("Solr")) {
      return new SolrIndexer(config, messenger, bypass, metricsPrefix, localRunId);
    } else if (typeName.equalsIgnoreCase("OpenSearch")) {
      return new OpenSearchIndexer(config, messenger, bypass, metricsPrefix, localRunId);
    } else if (typeName.equalsIgnoreCase("Elasticsearch")) {
      return new ElasticsearchIndexer(config, messenger, bypass, metricsPrefix, localRunId);
    } else if (typeName.equalsIgnoreCase("CSV")) {
      return new CSVIndexer(config, messenger, bypass, metricsPrefix, localRunId);
    } else {
      throw new IndexerException("Unknown indexer.type configuration of: '" + typeName + "'");
    }
  }
}
