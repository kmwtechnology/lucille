package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.*;

import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

public class TraversalParamsTest {

  private static final URI DEFAULT_URI = URI.create("s3://bucket/");
  private static final String DEFAULT_PREFIX = "test-";
  private static final Instant BASE_TIME = Instant.parse("2026-01-15T10:00:00Z");
  private static final Instant LATER_TIME = Instant.parse("2026-01-19T14:30:00Z");

  private TraversalParams params(Map<String, Object> config) {
    // helper to construct params with a small config override
    return new TraversalParams(ConfigFactory.parseMap(config), DEFAULT_URI, DEFAULT_PREFIX);
  }

  private TraversalParams emptyParams() {
    // baseline params with no config at all
    return new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
  }

  // basics tests
  @Test
  public void preservesDocIdPrefix() {
    // doc id prefix passed to the constructor should be used as-is
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, "prefix-");
    assertEquals("prefix-", params.getDocIdPrefix());
  }

  @Test
  public void preservesUri() {
    // traversal should retain the exact URI it was constructed with
    URI uri = URI.create("s3://my-bucket/path/");
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), uri, "");
    assertEquals(uri, params.getURI());
  }

  // file handlers tests

  @Test
  public void noExtensionsByDefault() {
    // with no handler config, no file extensions should be accepted
    assertFalse(emptyParams().supportedFileExtension("json"));
  }

  @Test
  public void extensionsFromConfig() {
    // configured handlers should be recognized, everything else rejected
    TraversalParams params =
        params(Map.of("fileHandlers", Map.of("json", Map.of(), "csv", Map.of())));
    assertTrue(params.supportedFileExtension("json"));
    assertTrue(params.supportedFileExtension("csv"));
    assertFalse(params.supportedFileExtension("xml"));
  }

  @Test
  public void handlerLookup() {
    // handler lookup should return a config entry only for known extensions
    TraversalParams params = params(Map.of("fileHandlers", Map.of("json", Map.of())));
    assertNotNull(params.handlerForExtension("json"));
    assertNull(params.handlerForExtension("csv"));
  }

  // incremental logic tests

  @Test
  public void futurePublishSkips() {
    // guardrail: if metadata claims a publish time in the future, skip the file
    Instant now = Instant.now();
    Instant future = now.plus(Duration.ofDays(1));
    assertFalse(emptyParams().includeFile("doc.json", now, future));
  }

  @Test
  public void nullPublishIncludes() {
    // brand new file with no prior publish timestamp should be included
    assertTrue(emptyParams().includeFile("doc.json", BASE_TIME, null));
  }

  @Test
  public void olderModifiedSkips() {
    // file modified before its last publish should not be reprocessed
    assertFalse(emptyParams().includeFile("doc.json", BASE_TIME, LATER_TIME));
  }

  @Test
  public void oneMsNewerIncludes() {
    // even a tiny timestamp bump counts as a real change
    assertTrue(emptyParams().includeFile("doc.json", BASE_TIME.plusMillis(1), BASE_TIME));
  }

  @Test
  public void skewedModifiedSkips() {
    // badly skewed timestamps far in the past should be treated as stale
    assertFalse(
        emptyParams().includeFile(
            "doc.json",
            Instant.parse("2020-01-01T00:00:00Z"),
            BASE_TIME
        )
    );
  }

  // incremental scenarios

  @Test
  public void freshIngestIncludesAll() {
    // first-time ingest should include every file
    TraversalParams params = emptyParams();
    for (String doc : List.of("doc_a.json", "doc_b.json", "doc_c.json")) {
      assertTrue(doc, params.includeFile(doc, BASE_TIME, null));
    }
  }

  @Test
  public void unchangedReingestSkipsAll() {
    // re-ingesting unchanged files should skip everything
    TraversalParams params = emptyParams();
    for (String doc : List.of("doc_a.json", "doc_b.json", "doc_c.json")) {
      assertFalse(doc, params.includeFile(doc, BASE_TIME, BASE_TIME));
    }
  }

  @Test
  public void singleChangeOnly() {
    // only the file with a newer modification should be included
    TraversalParams params = emptyParams();
    assertFalse(params.includeFile("doc_a.json", BASE_TIME, BASE_TIME));
    assertTrue(params.includeFile("doc_b.json", LATER_TIME, BASE_TIME));
    assertFalse(params.includeFile("doc_c.json", BASE_TIME, BASE_TIME));
  }

  @Test
  public void mixedNewAndUpdated() {
    // mix of unchanged, updated, and brand-new files in one pass
    TraversalParams params = emptyParams();
    Instant laterPub = Instant.parse("2026-01-17T14:00:00Z");
    Instant modified = Instant.parse("2026-01-19T17:00:00Z");

    assertFalse(params.includeFile("doc_a.json", BASE_TIME, BASE_TIME));
    assertFalse(params.includeFile("doc_b.json", laterPub, laterPub));
    assertTrue(params.includeFile("doc_c.json", modified, BASE_TIME));

    // files with no prior publish timestamp are treated as new
    for (String doc : List.of("doc_d.json", "doc_e.json", "doc_f.json")) {
      assertTrue(doc, params.includeFile(doc, modified, null));
    }
  }

  // edge cases

  @Test
  public void pathsWithSpecialChars() {
    // unusual but valid paths should not affect inclusion logic
    TraversalParams params = emptyParams();
    for (String path : List.of(
        "path/with spaces/doc.json",
        "path/with-dashes/doc.json",
        "path/with_underscores/doc.json"
    )) {
      assertTrue(path, params.includeFile(path, BASE_TIME, null));
    }
  }

  // parameterized tests

  @RunWith(Parameterized.class)
  public static class BooleanDefaultsTest {

    @Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return List.of(new Object[][]{
          {"handleArchivedFiles", false, "getHandleArchivedFiles"},
          {"handleCompressedFiles", false, "getHandleCompressedFiles"},
          {"getFileContent", true, "shouldGetFileContent"}
      });
    }

    private final String key;
    private final boolean expected;
    private final String method;

    public BooleanDefaultsTest(String key, boolean expected, String method) {
      this.key = key;
      this.expected = expected;
      this.method = method;
    }

    @Test
    public void usesDefault() throws Exception {
      // boolean options should have sensible defaults when not configured
      TraversalParams params =
          new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
      assertEquals(expected, TraversalParams.class.getMethod(method).invoke(params));
    }
  }

  @RunWith(Parameterized.class)
  public static class BooleanOverrideTest {

    @Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return List.of(new Object[][]{
          {"handleArchivedFiles", false, "getHandleArchivedFiles"},
          {"handleCompressedFiles", false, "getHandleCompressedFiles"},
          {"getFileContent", true, "shouldGetFileContent"}
      });
    }

    private final String key;
    private final boolean defaultValue;
    private final String method;

    public BooleanOverrideTest(String key, boolean defaultValue, String method) {
      this.key = key;
      this.defaultValue = defaultValue;
      this.method = method;
    }

    @Test
    public void respectsOverride() throws Exception {
      // explicit config values should override defaults
      TraversalParams params =
          new TraversalParams(
              ConfigFactory.parseMap(
                  Map.of("fileOptions", Map.of(key, !defaultValue))),
              DEFAULT_URI,
              DEFAULT_PREFIX);

      assertEquals(!defaultValue,
          TraversalParams.class.getMethod(method).invoke(params));
    }
  }

  @RunWith(Parameterized.class)
  public static class OptionalUriTest {

    @Parameters(name = "{index}: {0}")
    public static Iterable<Object[]> data() {
      return List.of(new Object[][]{
          {"moveToAfterProcessing", "getMoveToAfterProcessing"},
          {"moveToErrorFolder", "getMoveToErrorFolder"}
      });
    }

    private final String key;
    private final String method;

    public OptionalUriTest(String key, String method) {
      this.key = key;
      this.method = method;
    }

    @Test
    public void uriDefaultsToNull() throws Exception {
      // optional target URIs should default to null when unset
      TraversalParams params =
          new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
      assertNull(TraversalParams.class.getMethod(method).invoke(params));
    }

    @Test
    public void uriFromConfig() throws Exception {
      // configured target URIs should be parsed and returned
      TraversalParams params =
          new TraversalParams(
              ConfigFactory.parseMap(
                  Map.of("fileOptions", Map.of(key, "s3://target/"))),
              DEFAULT_URI,
              DEFAULT_PREFIX);

      assertEquals(
          URI.create("s3://target/"),
          TraversalParams.class.getMethod(method).invoke(params)
      );
    }
  }
}