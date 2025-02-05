package com.kmwllc.lucille.core.fileHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class JsonFileHandlerTest {

  @Test
  public void testProcessFileUsingPath() throws Exception {
    // testing prefix as well
    Config config = ConfigFactory.parseMap(Map.of("json", Map.of("docIdPrefix", "PREFIX")));
    FileHandler jsonHandler = FileHandler.create("jsonl", config);

    Path path = Paths.get("src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl");

    // contents of JSONConnectorTest/config.conf
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    Iterator<Document> docs = jsonHandler.processFile(path);
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
    FileHandler jsonHandler = FileHandler.create("jsonl", config);

    Path path = Paths.get("src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl");
    FileInputStream is = new FileInputStream(path.toFile());

    // contents of JSONConnectorTest/config.conf
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    Iterator<Document> docs = jsonHandler.processFile(is, path.toString());
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
    FileHandler jsonHandler = FileHandler.create("jsonl", config);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    Path path = Paths.get("src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl");

    jsonHandler.processFileAndPublish(publisher, path);

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

    FileHandler spyJsonHandler = spy(FileHandler.create("jsonl", config));
    Path path = Paths.get("src/test/resources/FileHandlerTest/JsonFileHandlerTest/default.jsonl");

    // note for this test we are arbitrarily creating fake documents with ids 1, 2, 3. We just want to test that document id 3
    // is published even though we encountered an error while iterating through the iterator
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2")).thenThrow(new RuntimeException("Iterator failed")).thenReturn(Document.create("3"));
    when(spyJsonHandler.processFile(path)).thenReturn(docs);
    doReturn(docs).when(spyJsonHandler).processFile(any());
    spyJsonHandler.processFileAndPublish(publisher, path);

    List<Document> docsPublished = messenger.getDocsSentForProcessing();

    // check that we published all documents even with Exception thrown while iterating through iterator
    assertEquals(3, docsPublished.size());

    // rest of the documents were published
    assertEquals("1", docsPublished.get(0).getId());
    assertEquals("2", docsPublished.get(1).getId());
    assertEquals("3", docsPublished.get(2).getId());
  }
}
