package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class IndexerFactory {
  public static final String DEFAULT_INDEXER_TYPE = "Solr";
  private static final Map<String, Class<? extends Indexer>> registeredIndexers = new HashMap<>();
  private static final Logger log = LoggerFactory.getLogger(Indexer.class);

  static {
    registerIndexer("Solr", SolrIndexer.class);
    registerIndexer("OpenSearch", OpenSearchIndexer.class);
  }

  public static void registerIndexer(String type, Class<? extends Indexer> _class) {
    registeredIndexers.put(type, _class);
  }

  public static Set<String> listIndexerTypes() {
    return registeredIndexers.keySet();
  }


  public static Class<? extends Indexer> getIndexerClass(String type) {
    return registeredIndexers.get(type);
  }

  /**
   * Instantiates an Indexer from the designated Config.
   */
  public static Indexer fromConfig(Config config, IndexerMessageManager manager, boolean bypass, String metricsPrefix) throws ClassNotFoundException, NoSuchMethodException,
      IllegalAccessException, InvocationTargetException, InstantiationException, IndexerException {
    String typeName;
    if (config.hasPath("indexer.type")) {
      typeName = config.getString("indexer.type");
    } else {
      log.info("Config setting for indexer.type was not specified, using default indexer type of '" + DEFAULT_INDEXER_TYPE + "'.");
      typeName = DEFAULT_INDEXER_TYPE;
    }
    Class<? extends Indexer> clazz = getIndexerClass(typeName);
    if (clazz == null) {
      String knownTypes = String.join(", ", listIndexerTypes());
      throw new IndexerException("Unknown indexer.type configuration of: '" + typeName +
          "'. Known Indexer types are: " + knownTypes);
    }
    Constructor<? extends Indexer> constructor = clazz
        .getConstructor(Config.class, IndexerMessageManager.class, boolean.class, String.class);
    return constructor.newInstance(config, manager, bypass, metricsPrefix);
  }

}
