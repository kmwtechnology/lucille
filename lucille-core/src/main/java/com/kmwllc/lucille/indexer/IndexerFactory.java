package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;

public class IndexerFactory {

  public static final String DEFAULT_INDEXER_TYPE = "Solr";
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

      String className = config.getString("indexer.class");

      try {
        Class<?> clazz = Class.forName(className);
        Constructor<?> constructor = clazz.getConstructor(Config.class, IndexerMessenger.class, Boolean.TYPE, String.class, String.class);
        return (Indexer) constructor.newInstance(config, messenger, bypass, metricsPrefix, localRunId);
      } catch (ReflectiveOperationException e) {
        throw new IndexerException("Problem initializing indexer.class configuration of: '" + className + "'", e);
      }

    } else if (config.hasPath("indexer.type")) {

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
}
