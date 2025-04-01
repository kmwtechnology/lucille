package com.kmwllc.lucille.core.configSpec;

import com.typesafe.config.Config;
import java.util.Set;

/**
 * A ConfigSpec for an Indexer.
 */
public class IndexerSpec extends BaseConfigSpec {

  // There aren't any common fields among various Indexer implementations.
  private static final Set<String> DEFAULT_INDEXER_PROPERTIES = Set.of();

  /**
   * Creates an IndexerSpec.
   */
  public IndexerSpec() { super(); }

  @Override
  protected Set<String> getDefaultSpecProperties() { return DEFAULT_INDEXER_PROPERTIES; }

  /**
   * Validates the given Indexer Config against the default properties / parents it is allowed to have.
   *
   * <p> This config should <b>not</b> be the Config for a specific implementation - it should be the general Config for
   * an Indexer, under key "indexer" in the Lucille Config.
   *
   * Throws an IllegalArgumentException if the Config contains an illegal property or is missing a required property.
   *
   * @param indexerConfig the general Config for an Indexer, under key "indexer" in the Lucille Config.
   */
  public static void validateGeneralIndexerConfig(Config indexerConfig) {
    ConfigSpec spec = ConfigSpec
        .withNoDefaults()
        .withOptionalProperties("type", "class", "idOverrideField", "indexOverrideField", "ignoreFields", "deletionMarkerField",
            "deletionMarkerFieldValue", "deleteByFieldField", "deleteByFieldValue", "batchSize", "batchTimeout", "logRate",
            "versionType", "routingField", "sendEnabled");

    spec.setDisplayName("Indexer");

    spec.validate(indexerConfig);
  }
}
