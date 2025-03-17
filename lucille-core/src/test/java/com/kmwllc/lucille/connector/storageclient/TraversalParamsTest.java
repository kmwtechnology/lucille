package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Map;
import org.junit.Test;

public class TraversalParamsTest {

  private final Path newFile = Paths.get("src/test/resources/TraversalParamsTest/new.txt");
  private final Path oldFile = Paths.get("src/test/resources/TraversalParamsTest/old.txt");

  private final URI testFolderURI = URI.create("src/test/resources/TraversalParamsTest");

  @Test
  public void testCutoffDefault() {
    // 1. No modificationCutoff - all files should be included, including files from Jan 1 1970 and right now.
    TraversalParams params = new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.empty());

    assertTrue(params.timeWithinCutoff(Instant.now()));
    assertTrue(params.timeWithinCutoff(Instant.ofEpochMilli(1)));

    // 2. A specified modificationCutoff, but no cutoffType - should automatically exclude files from BEFORE the cutoff.
    Map<String, String> filterOptionsMap = Map.of("modificationCutoff", "2 days");
    params = new TraversalParams(URI.create("src/test/resources/TraversalParamsTest"), "", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap));

    assertTrue(params.timeWithinCutoff(Instant.now()));
    assertFalse(params.timeWithinCutoff(Instant.ofEpochMilli(1)));
  }

  @Test
  public void testCutoffSpecifiedType() {
    Map<String, String> filterOptionsMap = Map.of(
        "modificationCutoff", "3 d",
        "cutoffType", "before"
    );
    TraversalParams params = new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap));

    assertTrue(params.timeWithinCutoff(Instant.now()));
    assertFalse(params.timeWithinCutoff(Instant.ofEpochMilli(1)));

    filterOptionsMap = Map.of(
        "modificationCutoff", "6h",
        "cutoffType", "AFTER"
    );
    params = new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap));

    assertFalse(params.timeWithinCutoff(Instant.now()));
    assertTrue(params.timeWithinCutoff(Instant.ofEpochMilli(1)));
  }

  @Test
  public void testCutoffInvalidConf() {
    Map<String, String> filterOptionsMap = Map.of(
        "modificationCutoff", "3 h",
        "cutoffType", "this-isn't-a-cutoff-type!"
    );

    assertThrows(IllegalArgumentException.class, () -> new TraversalParams(testFolderURI, "", ConfigFactory.empty(), ConfigFactory.parseMap(filterOptionsMap)));
  }
}
