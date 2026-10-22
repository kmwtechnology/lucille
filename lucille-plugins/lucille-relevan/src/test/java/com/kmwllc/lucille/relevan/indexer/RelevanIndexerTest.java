package com.kmwllc.lucille.relevan.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.relevan.indexer.FakeRelevan.Request;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RelevanIndexerTest {

  private static final String BULK = "/v1/test-org/indexes/songs/documents/_bulk";

  private FakeRelevan relevan;
  private TestMessenger messenger;

  @Before
  public void setUp() throws Exception {
    relevan = new FakeRelevan();
    messenger = new TestMessenger();
  }

  @After
  public void tearDown() {
    relevan.close();
  }

  private Config config(String extraRelevan, String extraIndexer) {
    return ConfigFactory.parseString(
        "indexer { class: \"com.kmwllc.lucille.relevan.indexer.RelevanIndexer\"\n batchSize: 2000\n " + extraIndexer + " }\n"
            + "relevan { apiKey: \"test-key\"\n index: \"songs\"\n orgId: \"test-org\"\n url: \"" + relevan.url() + "\"\n "
            + extraRelevan + " }");
  }

  private RelevanIndexer indexer(String extraRelevan, String extraIndexer) {
    return new RelevanIndexer(config(extraRelevan, extraIndexer), messenger, false, "testing", null);
  }

  private void index(RelevanIndexer indexer, Document... docs) throws Exception {
    for (Document doc : docs) {
      messenger.sendForIndexing(doc);
    }
    indexer.run(docs.length);
  }

  private static Document doc(String id, String title) {
    Document doc = Document.create(id, "run1");
    doc.setField("title", title);
    return doc;
  }

  private Map<String, Event.Type> eventTypes() {
    return messenger.getSentEvents().stream().collect(Collectors.toMap(Event::getDocumentId, Event::getType, (a, b) -> b));
  }

  private Event event(String docId) {
    return messenger.getSentEvents().stream().filter(e -> e.getDocumentId().equals(docId)).findFirst().orElseThrow();
  }

  private static List<String> ids(Request bulk) {
    return bulk.body().get("documents").findValuesAsText("id");
  }

  @Test
  public void testUpsertsBatchInOneBulkRequest() throws Exception {
    index(indexer("", ""), doc("a", "A"), doc("b", "B"), doc("c", "C"));

    List<Request> bulks = relevan.requests("POST", BULK);
    assertEquals(1, bulks.size());
    assertEquals(List.of("a", "b", "c"), ids(bulks.get(0)));
    assertEquals("test-key", bulks.get(0).apiKey());

    JsonNode content = bulks.get(0).body().get("documents").get(0).get("content");
    assertEquals("A", content.get("title").asText());
    assertFalse("Lucille's id field is not content", content.has("id"));
    assertFalse("Lucille's run_id field is not content", content.has("run_id"));

    assertEquals(Map.of("a", Event.Type.FINISH, "b", Event.Type.FINISH, "c", Event.Type.FINISH), eventTypes());
  }

  @Test
  public void testValidationErrorsFailOnlyRejectedDocuments() throws Exception {
    relevan.reply("POST", "/_bulk", 400, "{\"message\":\"One or more documents failed mapping validation.\",\"errors\":["
        + "{\"documentIndex\":1,\"field\":\"bpm\",\"message\":\"Field 'bpm' must be of kind 'number'.\"},"
        + "{\"documentIndex\":1,\"field\":\"x\",\"message\":\"Unknown field 'x'.\"}]}");

    index(indexer("", ""), doc("a", "A"), doc("b", "B"), doc("c", "C"));

    List<Request> bulks = relevan.requests("POST", BULK);
    assertEquals(2, bulks.size());
    assertEquals(List.of("a", "c"), ids(bulks.get(1)));
    assertEquals(Map.of("a", Event.Type.FINISH, "b", Event.Type.FAIL, "c", Event.Type.FINISH), eventTypes());
    assertTrue(event("b").getMessage().contains("Field 'bpm' must be of kind 'number'.; Unknown field 'x'."));
  }

  @Test
  public void testIdErrorsInValidationResponseFailOnlyThoseDocuments() throws Exception {
    // Relevan reports a malformed ID with its documentIndex, like any other field error. The indexer checks IDs
    // before sending, so this guards against Relevan's ID rules differing from the indexer's.
    relevan.reply("POST", "/_bulk", 400, "{\"message\":\"One or more documents are not valid. No documents were written.\","
        + "\"errors\":[{\"documentIndex\":0,\"field\":\"id\",\"message\":\"Invalid string: must match pattern /^[A-Za-z0-9_.:-]+$/\"}]}");

    index(indexer("", ""), doc("a", "A"), doc("b", "B"));

    assertEquals(List.of("b"), ids(relevan.requests("POST", BULK).get(1)));
    assertEquals(Map.of("a", Event.Type.FAIL, "b", Event.Type.FINISH), eventTypes());
    assertTrue(event("a").getMessage().contains("Invalid string: must match pattern"));
  }

  @Test
  public void testWholeRequestValidationFaultFailsWholeRequest() throws Exception {
    relevan.reply("POST", "/_bulk", 400, "{\"message\":\"The request is not valid. No documents were written. "
        + "documents: Too small: expected array to have >=1 items\",\"errors\":[]}");

    index(indexer("", ""), doc("a", "A"), doc("b", "B"));

    assertEquals(1, relevan.requests("POST", BULK).size());
    assertEquals(Map.of("a", Event.Type.FAIL, "b", Event.Type.FAIL), eventTypes());
    assertTrue(event("a").getMessage().contains("HTTP 400: The request is not valid."));
  }

  @Test
  public void testInvalidIdsFailWithoutBeingSent() throws Exception {
    index(indexer("", ""), doc("good", "A"), doc("path/to file.txt", "B"));

    assertEquals(List.of("good"), ids(relevan.requests("POST", BULK).get(0)));
    assertEquals(Event.Type.FAIL, event("path/to file.txt").getType());
    assertTrue(event("path/to file.txt").getMessage().contains("idOverrideField"));
  }

  @Test
  public void testIdOverrideField() throws Exception {
    Document doc = doc("path/to file.txt", "A");
    doc.setField("sku", "sku-1");

    index(indexer("", "idOverrideField: \"sku\""), doc);

    assertEquals(List.of("sku-1"), ids(relevan.requests("POST", BULK).get(0)));
    assertEquals(Event.Type.FINISH, event("path/to file.txt").getType());
  }

  @Test
  public void testRepeatedIdIsSentInOrderInSeparateRequests() throws Exception {
    index(indexer("", ""), doc("a", "first"), doc("b", "B"), doc("a", "second"));

    List<Request> bulks = relevan.requests("POST", BULK);
    assertEquals(2, bulks.size());
    assertEquals(List.of("a", "b"), ids(bulks.get(0)));
    assertEquals(List.of("a"), ids(bulks.get(1)));
    assertEquals("second", bulks.get(1).body().get("documents").get(0).get("content").get("title").asText());
  }

  @Test
  public void testDeletesById() throws Exception {
    Document deleted = Document.create("gone", "run1");
    deleted.setField("is_deleted", "true");
    Document alreadyGone = Document.create("never-existed", "run1");
    alreadyGone.setField("is_deleted", "true");
    relevan.reply("DELETE", "/documents/never-existed", 404, "{\"message\":\"Document 'never-existed' not found.\"}");

    index(indexer("", "deletionMarkerField: \"is_deleted\"\n deletionMarkerFieldValue: \"true\""),
        doc("kept", "K"), deleted, alreadyGone);

    assertEquals(List.of("kept"), ids(relevan.requests("POST", BULK).get(0)));
    assertEquals(1, relevan.requests("DELETE", "/v1/test-org/indexes/songs/documents/gone").size());
    assertEquals(Map.of("kept", Event.Type.FINISH, "gone", Event.Type.FINISH, "never-existed", Event.Type.FINISH),
        eventTypes());
  }

  @Test
  public void testDeleteThenUpsertOfSameIdKeepsOrder() throws Exception {
    Document deleted = Document.create("a", "run1");
    deleted.setField("is_deleted", "true");

    index(indexer("", "deletionMarkerField: \"is_deleted\"\n deletionMarkerFieldValue: \"true\""),
        doc("a", "old"), deleted, doc("a", "new"));

    List<String> calls = relevan.requests().stream().map(r -> r.method() + " " + r.path()).collect(Collectors.toList());
    assertEquals(List.of("POST " + BULK, "DELETE /v1/test-org/indexes/songs/documents/a", "POST " + BULK), calls);
  }

  @Test
  public void testFailedDeleteFailsDocument() throws Exception {
    Document deleted = Document.create("a", "run1");
    deleted.setField("is_deleted", "true");
    relevan.reply("DELETE", "/documents/a", 401, "{\"message\":\"Unauthorized\"}");

    index(indexer("", "deletionMarkerField: \"is_deleted\"\n deletionMarkerFieldValue: \"true\""), deleted);

    assertEquals(Event.Type.FAIL, event("a").getType());
    assertTrue(event("a").getMessage().contains("HTTP 401: Unauthorized"));
  }

  @Test
  public void testRateLimitIsRetried() throws Exception {
    relevan.reply("POST", "/_bulk", 429, "{\"message\":\"Too many requests\",\"code\":\"rate_limit_exceeded\"}");

    index(indexer("", "maxRetries: 2\n retryWaitDurationMs: 1"), doc("a", "A"));

    assertEquals(2, relevan.requests("POST", BULK).size());
    assertEquals(Event.Type.FINISH, event("a").getType());
  }

  @Test
  public void testServerErrorFailsWithoutRetriesConfigured() throws Exception {
    relevan.reply("POST", "/_bulk", 503, "<html>Service Unavailable</html>");

    index(indexer("", ""), doc("a", "A"));

    assertEquals(1, relevan.requests("POST", BULK).size());
    assertEquals(Event.Type.FAIL, event("a").getType());
    assertTrue(event("a").getMessage().contains("HTTP 503: <html>Service Unavailable</html>"));
  }

  @Test
  public void testStorageLimitIsNotRetried() throws Exception {
    relevan.reply("POST", "/_bulk", 402,
        "{\"message\":\"Storage limit exceeded\",\"code\":\"storage_limit_exceeded\",\"limitBytes\":1,\"usedBytes\":1}");

    index(indexer("", "maxRetries: 2\n retryWaitDurationMs: 1"), doc("a", "A"));

    assertEquals(1, relevan.requests("POST", BULK).size());
    assertTrue(event("a").getMessage().contains("HTTP 402 (storage_limit_exceeded): Storage limit exceeded"));
  }

  @Test
  public void testNetworkErrorFailsDocuments() throws Exception {
    RelevanIndexer indexer = indexer("", "");
    relevan.close();

    index(indexer, doc("a", "A"));

    assertEquals(Event.Type.FAIL, event("a").getType());
  }

  @Test
  public void testSplitsLargeBatchesIntoBulkRequestsOfAtMost1000() throws Exception {
    Document[] docs = new Document[1500];
    for (int i = 0; i < docs.length; i++) {
      docs[i] = doc("doc-" + i, "T" + i);
    }

    index(indexer("", ""), docs);

    List<Request> bulks = relevan.requests("POST", BULK);
    assertEquals(2, bulks.size());
    assertEquals(1000, bulks.get(0).body().get("documents").size());
    assertEquals(500, bulks.get(1).body().get("documents").size());
  }

  @Test
  public void testIndexOverrideField() throws Exception {
    Document other = doc("b", "B");
    other.setField("target", "other-index");

    index(indexer("", "indexOverrideField: \"target\""), doc("a", "A"), other);

    assertEquals(List.of("a"), ids(relevan.requests("POST", BULK).get(0)));
    assertEquals(List.of("b"), ids(relevan.requests("POST", "/v1/test-org/indexes/other-index/documents/_bulk").get(0)));
  }

  @Test
  public void testResolvesOrgIdFromApiKey() throws Exception {
    Config config = config("", "").withoutPath("relevan.orgId");

    index(new RelevanIndexer(config, messenger, false, "testing", null), doc("a", "A"), doc("b", "B"));

    assertEquals(1, relevan.requests("GET", "/v1/organizations").size());
    assertEquals(1, relevan.requests("POST", BULK).size());
  }

  @Test
  public void testDropUnmappedFieldsAndCoerceTypes() throws Exception {
    relevan.mapping("{\"fields\":{"
        + "\"title\":{\"use\":[\"searchable\"]},"
        + "\"bpm\":{\"use\":[\"sortable\"],\"kind\":\"number\"},"
        + "\"scores\":{\"use\":[\"filterable\"],\"kind\":\"number\"},"
        + "\"explicit\":{\"use\":[\"filterable\"],\"kind\":\"boolean\"},"
        + "\"code\":{\"use\":[\"filterable\"]}}}");
    Document doc = doc("a", "A");
    doc.setField("bpm", "140");
    doc.setField("explicit", "TRUE");
    doc.setField("code", "007");
    doc.setField("unmapped", "x");
    doc.addToField("scores", "1.5");
    doc.addToField("scores", "2");

    index(indexer("dropUnmappedFields: true\n coerceTypes: true", ""), doc, doc("b", "B"));

    JsonNode content = relevan.requests("POST", BULK).get(0).body().get("documents").get(0).get("content");
    assertEquals(140, content.get("bpm").asInt());
    assertTrue(content.get("bpm").isIntegralNumber());
    assertTrue(content.get("explicit").isBoolean());
    assertEquals("007", content.get("code").asText());
    assertEquals(1.5, content.get("scores").get(0).asDouble(), 0);
    assertEquals(2, content.get("scores").get(1).asInt());
    assertFalse(content.has("unmapped"));
    assertEquals(1, relevan.requests("GET", "/mappings").size());
  }

  @Test
  public void testCoerceLeavesUnparseableValues() {
    assertEquals("fast", RelevanIndexer.coerce("fast", "number"));
    assertEquals("NaN", RelevanIndexer.coerce("NaN", "number"));
    assertEquals("", RelevanIndexer.coerce("", "number"));
    assertEquals("yes", RelevanIndexer.coerce("yes", "boolean"));
    assertEquals(3L, RelevanIndexer.coerce(" 3 ", "number"));
    assertEquals("3", RelevanIndexer.coerce("3", "text"));
  }

  @Test
  public void testValidateConnection() {
    assertTrue(indexer("", "").validateConnection());
  }

  @Test
  public void testValidateConnectionFailsWithoutIndex() {
    relevan.reply("GET", "/indexes/songs", 404, "{\"message\":\"Index not found\"}");
    assertFalse(indexer("", "").validateConnection());
  }

  @Test
  public void testValidateConnectionFailsWithoutMapping() {
    relevan.mapping(null);
    assertFalse(indexer("", "").validateConnection());
  }

  @Test
  public void testBypassMakesNoRequests() {
    RelevanIndexer indexer = new RelevanIndexer(config("", ""), messenger, true, "testing", null);
    assertTrue(indexer.validateConnection());
    assertTrue(relevan.requests().isEmpty());
  }

  @Test
  public void testDeleteByQueryIsRejected() {
    assertThrows(IllegalArgumentException.class,
        () -> indexer("", "deleteByFieldField: \"f\"\n deleteByFieldValue: \"v\""));
  }

  @Test
  public void testInvalidConfigIsRejected() {
    assertThrows(Exception.class, () -> indexer("unknownSetting: true", ""));
    Config missingKey = config("", "").withoutPath("relevan.apiKey");
    assertThrows(Exception.class, () -> new RelevanIndexer(missingKey, messenger, false, "testing", null));
  }
}
