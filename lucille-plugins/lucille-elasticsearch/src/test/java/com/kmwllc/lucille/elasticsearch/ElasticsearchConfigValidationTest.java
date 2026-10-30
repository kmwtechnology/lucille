package com.kmwllc.lucille.elasticsearch;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Runner;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Validation-mode coverage for the Elasticsearch indexer's Spec. Moved from core's ConfigValidationTest once the
 * Elasticsearch indexer (and its Spec) were extracted into this plugin.
 */
public class ElasticsearchConfigValidationTest {

  @Test
  public void testBadIndexer() throws Exception {
    // When validating indexers, errors in the generic "indexer" block are reported before errors in the
    // specific-implementation ("elasticsearch") block. Here the elasticsearch block is missing the required "index"
    // property and misplaces "sendEnabled" (which belongs in the "indexer" block).
    Map<String, List<Exception>> exceptions = Runner.runInValidationMode("ElasticsearchConfigValidationTest/badIndexer.conf");
    assertTrue(exceptions.containsKey("indexer"));
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config contains unknown property sendEnabled"));
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config is missing required property index"));

    exceptions = Runner.runInValidationMode("ElasticsearchConfigValidationTest/extraIndexerProps.conf");
    assertTrue(exceptions.containsKey("indexer"));
    assertTrue(exceptions.get("indexer").get(0).getMessage().contains("Config contains unknown property blah"));
  }
}
