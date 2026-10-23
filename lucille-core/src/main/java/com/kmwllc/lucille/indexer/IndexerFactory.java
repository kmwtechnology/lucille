package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.typesafe.config.Config;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexerFactory {

  private static final Logger log = LoggerFactory.getLogger(IndexerFactory.class);

  /**
   * Name of the {@code public static final String} field an Indexer declares to register a
   * {@code indexer.type} name (e.g. "Solr"). Discovered reflectively, mirroring the existing
   * {@code public static Spec SPEC} convention. Indexers without this field simply cannot be selected
   * by {@code indexer.type} and must be selected by {@code indexer.class}.
   */
  private static final String INDEXER_TYPE_FIELD = "INDEXER_TYPE";

  /**
   * Instantiates an Indexer from the designated Config.
   *
   * <p>Selection precedence:
   * <ol>
   *   <li>{@code indexer.class} — fully-qualified class name, loaded reflectively.</li>
   *   <li>{@code indexer.type} — name resolved against Indexer implementations discovered on
   *       the classpath via {@link ServiceLoader}.</li>
   *   <li>Neither set — defaults to {@link NopIndexer}, which performs no indexing.</li>
   * </ol>
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
      // No indexer specified: default to a no-op indexer so a pipeline can run without a destination.
      log.warn("No indexer.class or indexer.type specified in Config. Defaulting to NopIndexer (no documents will be indexed).");
      return construct(NopIndexer.class, config, messenger, bypass, metricsPrefix, localRunId);
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
      Class<? extends Indexer> clazz = Class.forName(className).asSubclass(Indexer.class);
      return construct(clazz, config, messenger, bypass, metricsPrefix, localRunId);
    } catch (ReflectiveOperationException e) {
      throw new IndexerException("Problem initializing indexer.class configuration of: '" + className + "'", e);
    }
  }

  // Creates an indexer based on the config's "indexer.type" by matching the type name against Indexer
  // implementations discovered on the classpath via ServiceLoader. This is what allows backends that live in
  // separate plugin modules (Solr, OpenSearch, Elasticsearch) to keep the "indexer.type" shortcut working without
  // lucille-core referencing their classes by name.
  private static Indexer indexerFromType(Config config, IndexerMessenger messenger, boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    String typeName = config.getString("indexer.type");

    List<String> known = new ArrayList<>();
    for (IndexerTypeProvider provider : ServiceLoader.load(IndexerTypeProvider.class)) {
      Class<? extends Indexer> indexerClass = provider.getIndexerClass();
      String registeredType = readIndexerType(indexerClass);
      if (registeredType == null) {
        log.warn("IndexerTypeProvider {} provides {} which has no {} field; it cannot be selected by indexer.type.",
            provider.getClass().getName(), indexerClass.getName(), INDEXER_TYPE_FIELD);
        continue;
      }
      known.add(registeredType);
      if (registeredType.equalsIgnoreCase(typeName)) {
        return construct(indexerClass, config, messenger, bypass, metricsPrefix, localRunId);
      }
    }

    throw new IndexerException("Unknown indexer.type configuration of: '" + typeName + "'. Available types on the classpath: "
        + known + ". Ensure the module providing this indexer (e.g. lucille-solr, lucille-opensearch, lucille-elasticsearch) "
        + "is on the classpath, or specify a fully-qualified indexer.class instead.");
  }

  // Reads the static INDEXER_TYPE field from an Indexer class, or null if it does not declare one.
  private static String readIndexerType(Class<? extends Indexer> indexerClass) {
    try {
      Field field = indexerClass.getDeclaredField(INDEXER_TYPE_FIELD);
      Object value = field.get(null);
      return value == null ? null : value.toString();
    } catch (NoSuchFieldException e) {
      return null;
    } catch (IllegalAccessException e) {
      log.warn("Indexer {} declares a {} field that is not accessible; ignoring for indexer.type resolution.",
          indexerClass.getName(), INDEXER_TYPE_FIELD, e);
      return null;
    }
  }

  // Instantiates an Indexer via its standard (Config, IndexerMessenger, boolean, String, String) constructor.
  private static Indexer construct(Class<? extends Indexer> indexerClass, Config config, IndexerMessenger messenger,
      boolean bypass, String metricsPrefix, String localRunId) throws IndexerException {
    try {
      return indexerClass
          .getConstructor(Config.class, IndexerMessenger.class, Boolean.TYPE, String.class, String.class)
          .newInstance(config, messenger, bypass, metricsPrefix, localRunId);
    } catch (InvocationTargetException e) {
      // The constructor threw. Unwrap so the underlying cause (e.g. an IllegalArgumentException from Spec validation
      // with a message like "Config is missing required property index") surfaces with its own message instead of a
      // generic "Problem initializing" wrapper. This matches the behavior before indexers were constructed reflectively
      // via ServiceLoader: Spec/config validation throws unchecked exceptions, which we rethrow as-is.
      if (e.getCause() instanceof RuntimeException re) {
        throw re;
      }
      throw new IndexerException("Problem initializing indexer of class: '" + indexerClass.getName() + "'", e.getCause());
    } catch (ReflectiveOperationException e) {
      throw new IndexerException("Problem initializing indexer of class: '" + indexerClass.getName() + "'", e);
    }
  }
}
