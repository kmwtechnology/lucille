package com.kmwllc.lucille.core.filehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JsonFileHandlerTest {

  @Test
  public void testProcessFileUsingPath() throws Exception {
    // testing prefix as well
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "PREFIX")));
    FileHandler jsonHandler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);

    // contents of FileHandlerTest/JsonFileHandlerTest/default.jsonl
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    Iterator<Document> docs = jsonHandler.processFile(new FileInputStream(file), filePath);
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    // prefix should be applied to doc ids and run_id is not present as it is not published yet
    Document doc1Expected = Document.createFromJson(
        "{\"id\": \"PREFIX1\", \"field1\":\"val1-1\", \"field2\":[\"val2-1a\", \"val2-1b\"]}");
    Document doc2Expected = Document.createFromJson(
        "{\"id\": \"PREFIX2\", \"field3\":\"val3\", \"field2\":[\"val2-2a\", \"val2-2b\"]}");
    Document doc3Expected = Document.createFromJson("{\"id\": \"PREFIX3\", \"field4\":\"val4\", \"field5\":\"val5\"}");
    assertEquals(doc1Expected, doc1);
    assertEquals(doc2Expected, doc2);
    assertEquals(doc3Expected, doc3);
  }

  @Test
  public void testProcessFileUsingBytes() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "PREFIX")));
    FileHandler jsonHandler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);

    // contents of FileHandlerTest/JsonFileHandlerTest/default.jsonl
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    Iterator<Document> docs = jsonHandler.processFile(new FileInputStream(file), filePath);
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    // prefix should be applied to doc ids and run_id is not present as it is not published yet
    Document doc1Expected = Document.createFromJson(
        "{\"id\": \"PREFIX1\", \"field1\":\"val1-1\", \"field2\":[\"val2-1a\", \"val2-1b\"]}");
    Document doc2Expected = Document.createFromJson(
        "{\"id\": \"PREFIX2\", \"field3\":\"val3\", \"field2\":[\"val2-2a\", \"val2-2b\"]}");
    Document doc3Expected = Document.createFromJson("{\"id\": \"PREFIX3\", \"field4\":\"val4\", \"field5\":\"val5\"}");
    assertEquals(doc1Expected, doc1);
    assertEquals(doc2Expected, doc2);
    assertEquals(doc3Expected, doc3);
  }

  @Test
  public void testProcessAndPublishFileSuccessful() throws Exception {
    // testing prefix as well
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "PREFIX")));
    FileHandler jsonHandler = FileHandler.create("json", config);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);

    jsonHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    // prefix should be applied to doc ids and run_id should be added
    Document doc1 = Document.createFromJson(
        "{\"id\": \"PREFIX1\", \"field1\":\"val1-1\", \"field2\":[\"val2-1a\", \"val2-1b\"],\"run_id\":\"run1\"}");
    Document doc2 = Document.createFromJson(
        "{\"id\": \"PREFIX2\", \"field3\":\"val3\", \"field2\":[\"val2-2a\", \"val2-2b\"],\"run_id\":\"run1\"}");
    Document doc3 = Document.createFromJson("{\"id\": \"PREFIX3\", \"field4\":\"val4\", \"field5\":\"val5\",\"run_id\":\"run1\"}");
    assertEquals(doc1, docs.get(0));
    assertEquals(doc2, docs.get(1));
    assertEquals(doc3, docs.get(2));
  }

  @Test
  public void testProcessFileWithIteratorExhaustionFailure() throws Exception {
    // testing prefix as well
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "PREFIX")));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler spyJsonHandler = spy(FileHandler.create("json", config));
    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);
    FileInputStream stream = new FileInputStream(file);

    // note for this test we are arbitrarily creating fake documents with ids 1, 2, 3. We just want to test that document id 3
    // is published even though we encountered an error while iterating through the iterator
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2")).thenThrow(new RuntimeException("Iterator failed"))
        .thenReturn(Document.create("3"));
    when(spyJsonHandler.processFile(stream, filePath)).thenReturn(docs);
    doReturn(docs).when(spyJsonHandler).processFile(stream, filePath);
    spyJsonHandler.processFileAndPublish(publisher, stream, filePath);

    List<Document> docsPublished = messenger.getDocsSentForProcessing();

    // check that we published all documents even with Exception thrown while iterating through iterator
    assertEquals(3, docsPublished.size());

    // rest of the documents were published
    assertEquals("1", docsPublished.get(0).getId());
    assertEquals("2", docsPublished.get(1).getId());
    assertEquals("3", docsPublished.get(2).getId());
  }

  @Test
  public void testSingleIdField() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of("idField", "field1", "docIdPrefix", "")
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    Document d1 = docs.next(), d2 = docs.next(), d3 = docs.next();
    assertEquals("one", d1.getId());
    assertEquals("two", d2.getId());
    assertEquals("three", d3.getId());
    assertFalse(docs.hasNext());
  }

  private static InputStream stream(String content) {
    return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
  }

  private static List<String> idsOf(Iterator<Document> docs) {
    List<String> ids = new ArrayList<>();
    while (docs.hasNext()) {
      ids.add(docs.next().getId());
    }
    return ids;
  }

  @Test
  public void testBadLinesAreSkippedAndTheRestAreRead() throws Exception {
    // a line whose data cannot be turned into a Document is skipped; it does not end the file
    FileHandler handler = FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "p-"))));
    String content = String.join("\n",
        "{\"id\":\"a\"}",
        "{\"x\":1}",                // no id
        "{\"id\":null}",
        "{\"id\":\"\"}",
        "{\"id\":\"   \"}",         // blank
        "{\"id\":5}",               // not a string
        "[\"not\", \"an\", \"object\"]",
        "{not json",
        "{\"id\":\"c\"}") + "\n";

    assertEquals(List.of("p-a", "p-c"), idsOf(handler.processFile(stream(content), "inline.jsonl")));
  }

  @Test
  public void testBadLinesAreSkippedWithIdFields() throws Exception {
    FileHandler handler = FileHandler.create("json",
        ConfigFactory.parseMap(Map.of("json", Map.of("idField", "field1", "docIdPrefix", "p-"))));
    String content = String.join("\n",
        "{\"field1\":\"a\"}",
        "{\"other\":1}",            // id field missing
        "{\"field1\":null}",
        "{\"field1\":\"\"}",
        "{\"field1\":\"  \"}",      // blank
        "[1]",
        "{not json",
        "{\"field1\":\"c\"}") + "\n";

    assertEquals(List.of("p-a", "p-c"), idsOf(handler.processFile(stream(content), "inline.jsonl")));
  }

  @Test
  public void testBadLinesDoNotStopThePublishingOfTheRest() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    FileHandler handler = FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of())));

    handler.processFileAndPublish(publisher, stream("{\"id\":\"a\"}\n{\"x\":1}\n{\"id\":\"c\"}\n{\"id\":\"d\"}\n"), "inline.jsonl");

    List<String> published = messenger.getDocsSentForProcessing().stream().map(Document::getId).toList();
    assertEquals(List.of("a", "c", "d"), published);
  }

  @Test
  public void testBadLastLine() throws Exception {
    // skipping a bad line at the end leaves nothing more to read, without an error
    FileHandler handler = FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of())));
    Iterator<Document> docs = handler.processFile(stream("{\"id\":\"a\"}\n{\"x\":1}\n"), "inline.jsonl");

    assertEquals("a", docs.next().getId());
    assertFalse(docs.hasNext());
    assertFalse(docs.hasNext());
  }

  @Test
  public void testNextWithoutHasNext() throws Exception {
    FileHandler handler = FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of())));
    Iterator<Document> docs = handler.processFile(stream("{\"x\":1}\n{\"id\":\"a\"}\n"), "inline.jsonl");

    assertEquals("a", docs.next().getId());
    assertThrows(NoSuchElementException.class, docs::next);
  }

  @Test
  public void testTooManyConsecutiveBadLinesFailsTheFile() throws Exception {
    FileHandler handler = FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of())));
    String bad = "{\"x\":1}\n";
    String good = "{\"id\":\"ok\"}\n";
    int limit = BadRecordTracker.MAX_CONSECUTIVE;

    // one fewer than the limit, repeatedly, is fine because a good line resets the count
    String content = bad.repeat(limit - 1) + good + bad.repeat(limit - 1) + good;
    assertEquals(List.of("ok", "ok"), idsOf(handler.processFile(stream(content), "inline.jsonl")));

    // the limit reached: the iterator fails, and so does publishing the file, rather than reading on
    String tooMany = bad.repeat(limit) + good;
    Iterator<Document> docs = handler.processFile(stream(tooMany), "inline.jsonl");
    assertThrows(BadRecordTracker.TooManyBadRecordsException.class, docs::hasNext);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    assertThrows(FileHandlerException.class, () -> handler.processFileAndPublish(publisher, stream(tooMany), "inline.jsonl"));
    assertEquals(0, messenger.getDocsSentForProcessing().size());
  }

  @Test
  public void testCompositeIdFieldsDefaultJoin() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of("idFields", List.of("field1", "field2"), "docIdPrefix", "")
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    assertEquals("one_1", docs.next().getId());
    assertEquals("two_2", docs.next().getId());
    assertEquals("three_3", docs.next().getId());
    assertFalse(docs.hasNext());
  }

  @Test
  public void testBlacklist() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "blacklist", List.of("field2"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    Document doc1 = docs.next();
    assertEquals("1", doc1.getId());
    assertEquals("val1-1", doc1.getString("field1"));
    assertFalse(doc1.has("field2"));
  }

  @Test
  public void testWhitelist() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "whitelist", List.of("field1", "id"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    Document doc1 = docs.next();
    assertEquals("1", doc1.getId());
    assertEquals("val1-1", doc1.getString("field1"));
    assertFalse(doc1.has("field2"));
  }

  @Test
  public void testBlacklistWithIdFieldsConfigured() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idFields", List.of("field1", "field2"),
            "blacklist", List.of("field3", "id"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    Document doc1 = docs.next();
    assertEquals("one_1", doc1.getId());
    assertEquals("one", doc1.getString("field1"));
    assertEquals("1", doc1.getString("field2"));
    assertFalse(doc1.has("field3"));
  }

  @Test
  public void testWhitelistWithIdFieldsConfigured() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idFields", List.of("field1", "field2"),
            "whitelist", List.of("field1", "field2", "id"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);

    Document doc1 = docs.next();
    assertEquals("one_1", doc1.getId());
    assertEquals("one", doc1.getString("field1"));
    assertEquals("1", doc1.getString("field2"));
    assertFalse(doc1.has("field3"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIdFieldBlacklisted() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idFields", List.of("field1", "field2"),
            "blacklist", List.of("field1"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIdFieldNotWhitelisted() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idFields", List.of("field1", "field2"),
            // no field2
            "whitelist", List.of("field1"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIdBlacklistedWithNoIdFields() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "blacklist", List.of("id"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIdNotWhitelistedWithNoIdFields() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "whitelist", List.of("field1"),
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);
  }

  @Test
  public void testCompositeWithDocIdFormat() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idFields", List.of("field2", "field2"),
            "docIdFormat", "%s-%s",
            "docIdPrefix", ""
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    assertEquals("1-1", docs.next().getId());
    assertEquals("2-2", docs.next().getId());
    assertEquals("3-3", docs.next().getId());
    assertFalse(docs.hasNext());
  }

  @Test
  public void testIdFieldPrecedenceOverIdFields() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "idField", "field1",
            "idFields", List.of("field2", "field2")
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    assertEquals("one", docs.next().getId());
    assertEquals("two", docs.next().getId());
    assertEquals("three", docs.next().getId());
    assertFalse(docs.hasNext());
  }

  @Test
  public void testMissingCompositeFieldProducesBlankSegment() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of("idFields", List.of("field2", "field3"))
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    assertEquals("1_test", docs.next().getId()); // first line has field3
    assertEquals("2_", docs.next().getId()); // second line missing field3
    assertEquals("3_", docs.next().getId()); // third line missing field3
    assertFalse(docs.hasNext());
  }

  @Test
  public void testDocIdPrefixWithCompositeKey() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of(
        "json", Map.of(
            "docIdPrefix", "PREFIX-",
            "idFields", List.of("field1", "field2")
        )
    ));
    FileHandler handler = FileHandler.create("json", config);

    String filePath = "src/test/resources/FileHandlerTest/JsonFileHandlerTest/noids.jsonl";
    File file = new File(filePath);

    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
    assertEquals("PREFIX-one_1", docs.next().getId());
    assertEquals("PREFIX-two_2", docs.next().getId());
    assertEquals("PREFIX-three_3", docs.next().getId());
    assertFalse(docs.hasNext());
  }

  @Test
  public void testMaxRecordBytes() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("maxRecordBytes", 30)));
    FileHandler handler = FileHandler.create("json", config);

    String input = "{\"id\": \"1\", \"a\": \"short\"}\n"
        + "{\"id\": \"2\", \"a\": \"this line is longer than the limit\"}\n"
        + "{\"id\": \"3\", \"a\": \"short\"}\n";

    Iterator<Document> docs = handler.processFile(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), "test.jsonl");
    assertEquals("1", docs.next().getId());

    IllegalStateException e = assertThrows(IllegalStateException.class, docs::hasNext);
    assertTrue(e.getMessage().contains("line 2"));
    assertTrue(e.getMessage().contains("test.jsonl"));
    assertFalse(docs.hasNext());
  }

  @Test
  public void testMaxRecordBytesStillAppliesWhileSkippingBadLines() throws Exception {
    // lines 2 and 3 have no id and are skipped; line 4 is over the limit and fails the file
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("maxRecordBytes", 30)));
    FileHandler handler = FileHandler.create("json", config);

    String input = "{\"id\": \"1\"}\n"
        + "{\"x\": 1}\n"
        + "{\"x\": 2}\n"
        + "{\"id\": \"2\", \"a\": \"this line is longer than the limit\"}\n"
        + "{\"id\": \"3\"}\n";

    Iterator<Document> docs = handler.processFile(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)), "test.jsonl");
    assertEquals("1", docs.next().getId());

    IllegalStateException e = assertThrows(IllegalStateException.class, docs::hasNext);
    assertFalse(e instanceof BadRecordTracker.TooManyBadRecordsException);
    assertTrue(e.getMessage(), e.getMessage().contains("line 4"));
    assertFalse(docs.hasNext());
  }

  @Test
  public void testInvalidMaxRecordBytes() {
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("maxRecordBytes", 0)));
    assertThrows(IllegalArgumentException.class, () -> FileHandler.create("json", config));
  }
}
