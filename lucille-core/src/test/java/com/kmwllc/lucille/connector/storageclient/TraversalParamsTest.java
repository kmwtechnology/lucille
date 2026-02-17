package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.typesafe.config.ConfigFactory;

public class TraversalParamsTest {

  private static final URI DEFAULT_URI = URI.create("s3://bucket/");
  private static final String DEFAULT_PREFIX = "test-";
  private static final Instant BASE_TIME = Instant.parse("2026-01-15T10:00:00Z");
  private static final Instant LATER_TIME = Instant.parse("2026-01-19T14:30:00Z");

  // one table row for includefile behavior plus optional cutoff config.
  private record IncludeFileCase(
      String name, String publishMode,
      Instant fileLastModified, Instant fileLastPublished,
      String lastModifiedCutoff, String lastPublishedCutoff,
      boolean expected) {}

  private TraversalParams params(Map<String, Object> config) {
    // helper to construct params with a small config override
    return new TraversalParams(ConfigFactory.parseMap(config), DEFAULT_URI, DEFAULT_PREFIX);
  }

  private TraversalParams emptyParams() {
    // baseline params with no config at all
    return new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
  }

  private TraversalParams incrementalParams() {
    return params(Map.of("filterOptions", Map.of("publishMode", "incremental")));
  }

  private TraversalParams paramsWithFilterOptions(Map<String, Object> filterOptions) {
    return params(Map.of("filterOptions", filterOptions));
  }

  private TraversalParams paramsWithFileOptions(Map<String, Object> fileOptions) {
    return params(Map.of("fileOptions", fileOptions));
  }

  private void assertAllIncluded(TraversalParams params, Instant modified, Instant published, List<String> docs) {
    for (String doc : docs) {
      assertTrue(doc, params.includeFile(doc, modified, published));
    }
  }

  private void assertAllExcluded(TraversalParams params, Instant modified, Instant published, List<String> docs) {
    for (String doc : docs) {
      assertFalse(doc, params.includeFile(doc, modified, published));
    }
  }

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
    // if last published is in the future, treat it as already published and skip.
    Instant future = BASE_TIME.plus(Duration.ofDays(1));
    assertFalse(incrementalParams().includeFile("doc.json", BASE_TIME, future));
  }

  @Test
  public void nullPublishIncludes() {
    // files with no publish history should be considered new.
    assertTrue(incrementalParams().includeFile("doc.json", BASE_TIME, null));
  }

  @Test
  public void olderModifiedSkips() {
    // unchanged files should be skipped in incremental mode.
    assertFalse(incrementalParams().includeFile("doc.json", BASE_TIME, LATER_TIME));
  }

  @Test
  public void oneMsNewerIncludes() {
    // any forward movement in modified time should trigger inclusion.
    assertTrue(incrementalParams().includeFile("doc.json", BASE_TIME.plusMillis(1), BASE_TIME));
  }

  @Test
  public void skewedModifiedSkips() {
    // badly skewed timestamps far in the past should be treated as stale
    assertFalse(
        incrementalParams().includeFile(
            "doc.json",
            Instant.parse("2020-01-01T00:00:00Z"),
            BASE_TIME
        )
    );
  }

  // incremental scenarios

  @Test
  public void freshIngestIncludesAll() {
    // first traversal has no publish history, so all files should pass.
    TraversalParams params = incrementalParams();
    assertAllIncluded(params, BASE_TIME, null, List.of("doc_a.json", "doc_b.json", "doc_c.json"));
  }

  @Test
  public void unchangedReingestSkipsAll() {
    // second traversal with identical modified timestamps should skip all.
    TraversalParams params = incrementalParams();
    assertAllExcluded(params, BASE_TIME, BASE_TIME, List.of("doc_a.json", "doc_b.json", "doc_c.json"));
  }

  @Test
  public void singleChangeOnly() {
    // only the doc with a newer modified timestamp should pass.
    TraversalParams params = incrementalParams();
    assertFalse(params.includeFile("doc_a.json", BASE_TIME, BASE_TIME));
    assertTrue(params.includeFile("doc_b.json", LATER_TIME, BASE_TIME));
    assertFalse(params.includeFile("doc_c.json", BASE_TIME, BASE_TIME));
  }

  @Test
  public void mixedNewAndUpdated() {
    // unchanged, updated, and brand-new files should be handled in one pass.
    TraversalParams params = incrementalParams();
    Instant laterPub = Instant.parse("2026-01-17T14:00:00Z");
    Instant modified = Instant.parse("2026-01-19T17:00:00Z");

    assertFalse(params.includeFile("doc_a.json", BASE_TIME, BASE_TIME));
    assertFalse(params.includeFile("doc_b.json", laterPub, laterPub));
    assertTrue(params.includeFile("doc_c.json", modified, BASE_TIME));

    assertAllIncluded(params, modified, null, List.of("doc_d.json", "doc_e.json", "doc_f.json"));
  }

  // edge cases

  @Test
  public void pathsWithSpecialChars() {
    // unusual but valid paths should not affect inclusion logic
    TraversalParams params = emptyParams();
    assertAllIncluded(params, BASE_TIME, null, List.of(
        "path/with spaces/doc.json",
        "path/with-dashes/doc.json",
        "path/with_underscores/doc.json"
    ));
  }

  @Test
  public void includeFileModeMatrix() {
    // cutoff filters are relative to now, so this matrix intentionally anchors around current time.
    Instant now = Instant.now();
    Instant modifiedOld = now.minus(Duration.ofDays(10));
    Instant modifiedNew = now;
    Instant publishedRecent = now.minus(Duration.ofHours(1));
    Instant publishedOld = now.minus(Duration.ofDays(7));

    List<IncludeFileCase> cases = List.of(
        new IncludeFileCase("full_includes_unchanged", "full", modifiedOld, publishedRecent, null, null, true),
        new IncludeFileCase("incremental_skips_unchanged", "incremental", modifiedOld, publishedRecent, null, null, false),
        new IncludeFileCase("incremental_includes_new", "incremental", modifiedOld, null, null, null, true),
        new IncludeFileCase("incremental_includes_modified", "incremental", modifiedNew, publishedRecent, null, null, true),
        new IncludeFileCase("full_respects_last_modified_cutoff", "full", modifiedOld, null, "24h", null, false),
        new IncludeFileCase("incremental_respects_last_modified_cutoff", "incremental", modifiedOld, null, "24h", null, false),
        new IncludeFileCase("full_ignores_last_published_cutoff", "full", modifiedNew, publishedRecent, null, "24h", true),
        new IncludeFileCase("incremental_respects_last_published_cutoff", "incremental", modifiedNew, publishedRecent, null, "24h", false),
        new IncludeFileCase("incremental_allows_old_publish_by_cutoff", "incremental", modifiedNew, publishedOld, null, "24h", true)
    );

    for (IncludeFileCase testCase : cases) {
      Map<String, Object> filterOptions = new java.util.LinkedHashMap<>();
      filterOptions.put("publishMode", testCase.publishMode());
      if (testCase.lastModifiedCutoff() != null) {
        filterOptions.put("lastModifiedCutoff", testCase.lastModifiedCutoff());
      }
      if (testCase.lastPublishedCutoff() != null) {
        filterOptions.put("lastPublishedCutoff", testCase.lastPublishedCutoff());
      }

      TraversalParams params = paramsWithFilterOptions(filterOptions);

      assertEquals(
          testCase.name(),
          testCase.expected(),
          params.includeFile("doc.json", testCase.fileLastModified(), testCase.fileLastPublished()));
    }
  }

  @Test
  public void booleanOptionDefaults() {
    // verify defaults match expected file option behavior.
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
    assertFalse(params.getHandleArchivedFiles());
    assertFalse(params.getHandleCompressedFiles());
    assertTrue(params.shouldGetFileContent());
  }

  @Test
  public void booleanOptionsRespectOverride() {
    // explicit file option config should override defaults.
    TraversalParams params = paramsWithFileOptions(Map.of(
            "handleArchivedFiles", true,
            "handleCompressedFiles", true,
            "getFileContent", false));
    assertTrue(params.getHandleArchivedFiles());
    assertTrue(params.getHandleCompressedFiles());
    assertFalse(params.shouldGetFileContent());
  }

  @Test
  public void optionalUrisDefaultToNull() {
    // optional file move targets are unset unless explicitly configured.
    TraversalParams params = new TraversalParams(ConfigFactory.empty(), DEFAULT_URI, DEFAULT_PREFIX);
    assertNull(params.getMoveToAfterProcessing());
    assertNull(params.getMoveToErrorFolder());
  }

  @Test
  public void optionalUrisReadFromConfig() {
    // optional file move targets should parse when configured.
    TraversalParams moveToAfter = paramsWithFileOptions(Map.of("moveToAfterProcessing", "s3://target/"));
    assertEquals(URI.create("s3://target/"), moveToAfter.getMoveToAfterProcessing());

    TraversalParams moveToError = paramsWithFileOptions(Map.of("moveToErrorFolder", "s3://target/"));
    assertEquals(URI.create("s3://target/"), moveToError.getMoveToErrorFolder());
  }
}
