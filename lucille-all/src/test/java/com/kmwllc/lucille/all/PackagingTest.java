package com.kmwllc.lucille.all;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.indexer.IndexerFactory;
import com.kmwllc.lucille.indexer.IndexerTypeProvider;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import org.junit.Test;

/**
 * Packaging sanity check for the lucille-all bundle.
 *
 * <p>lucille-all puts lucille-core and every component plugin on a single classpath. Bundling everything together can
 * break in ways that no single-plugin build would catch:
 * <ul>
 *   <li>Each plugin ships its own {@code META-INF/services/com.kmwllc.lucille.indexer.IndexerTypeProvider}; if these
 *       are not merged correctly, some indexer types silently disappear.</li>
 *   <li>Conflicting transitive dependencies across plugins can make classes fail to load at runtime.</li>
 * </ul>
 *
 * <p>These tests assert that, with the whole bundle on the classpath, every {@code indexer.type} resolves to the
 * expected implementation through {@link IndexerFactory}. They construct indexers in bypass mode, so no external
 * service (Solr, OpenSearch, Elasticsearch) is contacted.
 */
public class PackagingTest {

  // indexer.type -> the fully-qualified class it must resolve to on the combined classpath.
  private static final Map<String, String> EXPECTED_TYPES = Map.of(
      "Solr", "com.kmwllc.lucille.solr.indexer.SolrIndexer",
      "OpenSearch", "com.kmwllc.lucille.opensearch.indexer.OpenSearchIndexer",
      "Elasticsearch", "com.kmwllc.lucille.elasticsearch.indexer.ElasticsearchIndexer",
      "Nop", "com.kmwllc.lucille.indexer.NopIndexer");

  @Test
  public void testEveryIndexerTypeResolvesOnCombinedClasspath() throws Exception {
    for (Map.Entry<String, String> entry : EXPECTED_TYPES.entrySet()) {
      String type = entry.getKey();
      String expectedClass = entry.getValue();

      Map<String, Object> settings = new HashMap<>();
      settings.put("indexer.type", type);
      settings.put("indexer.sendEnabled", false);
      // Minimal backend config so Spec validation passes for the search backends.
      settings.put("solr.url", "http://localhost:8983/solr");
      settings.put("opensearch.url", "http://localhost:9200");
      settings.put("opensearch.index", "test");
      settings.put("elasticsearch.url", "http://localhost:9200");
      settings.put("elasticsearch.index", "test");

      Config config = ConfigFactory.parseMap(settings);
      // bypass=true so no client is created / no network call is made. Only check the resolved type.
      Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "packaging-test");
      assertEquals(expectedClass, indexer.getClass().getName());
    }
  }

  /**
   * Directly asserts the ServiceLoader sees every backend provider once the whole bundle is on the classpath. This is
   * the registration layer underneath {@link IndexerFactory}.
   */
  @Test
  public void testAllBackendProvidersAreDiscovered() {
    List<String> discoveredTypes = new ArrayList<>();
    for (IndexerTypeProvider provider : ServiceLoader.load(IndexerTypeProvider.class)) {
      discoveredTypes.add(provider.getIndexerClass().getName());
    }

    List<String> expected = List.of(
        "com.kmwllc.lucille.indexer.CSVIndexer",
        "com.kmwllc.lucille.indexer.NopIndexer",
        "com.kmwllc.lucille.solr.indexer.SolrIndexer",
        "com.kmwllc.lucille.opensearch.indexer.OpenSearchIndexer",
        "com.kmwllc.lucille.elasticsearch.indexer.ElasticsearchIndexer");

    for (String expectedProvider : expected) {
      assertTrue("Expected IndexerTypeProvider for " + expectedProvider + " to be discoverable on the combined "
          + "classpath, but only found: " + discoveredTypes, discoveredTypes.contains(expectedProvider));
    }
  }
}
