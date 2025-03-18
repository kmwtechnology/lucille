package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Instant;
import java.util.Map;
import org.junit.Test;

public class TraversalParamsTest {

  private final URI testFolderURI = URI.create("src/test/resources/TraversalParamsTest");

  @Test
  public void testModificationCutoff() {
    // 1. No modificationCutoff - all files should be included, including files from Jan 1 1970 and right now.
    TraversalParams params = new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.empty());

    assertTrue(params.timeWithinCutoff(Instant.now()));
    assertTrue(params.timeWithinCutoff(Instant.ofEpochMilli(1)));

    // 2. A specified modificationCutoff, should exclude files from BEFORE the cutoff.
    Map<String, String> filterOptionsMap = Map.of("modificationCutoff", "2 days");
    params = new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap));

    assertTrue(params.timeWithinCutoff(Instant.now()));
    assertFalse(params.timeWithinCutoff(Instant.ofEpochMilli(1)));
  }
}
