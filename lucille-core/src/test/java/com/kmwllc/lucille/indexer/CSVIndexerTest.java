package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CSVIndexerTest {

  private final File outputFile = new File("output.csv");

  @Before
  public void init() {
    outputFile.delete();
  }

  @After
  public void teardown() {
    outputFile.delete();
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to
   * Elasticsearch.
   *
   * @throws Exception
   */
  @Test
  public void testCSVIndexer() throws Exception {
    assertFalse(outputFile.exists());

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    doc.setField("f1", "123");
    doc.setField("f2", "abc");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.setField("f1", "456");
    doc2.setField("f2", "def");

    CSVIndexer indexer = new CSVIndexer(config, messenger, false, "testing");
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }

    List<String> lines = Files.readAllLines(outputFile.toPath());
    assertEquals(2, lines.size());
    assertEquals("\"doc1\",\"123\",\"abc\"", lines.get(0));
    assertEquals("\"doc2\",\"456\",\"def\"", lines.get(1));
  }

  @Test
  public void testFlushesAfterSend() throws Exception {
    assertFalse(outputFile.exists());

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    doc.setField("f1", "123");
    doc.setField("f2", "abc");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.setField("f1", "456");
    doc2.setField("f2", "def");
    CSVIndexer indexer = null;
    List<String> lines;
    try {
      indexer = new CSVIndexer(config, messenger, false, "testing");
      // uses sendToIndex() rather than run() to demonstrate the flushing behavior before the indexer is shut down.
      indexer.sendToIndex(List.of(doc, doc2));
      lines = Files.readAllLines(outputFile.toPath());
    } finally {
      if (indexer != null) {
        indexer.closeConnection();
      }
    }
    assertEquals(2, lines.size());
    assertEquals("\"doc1\",\"123\",\"abc\"", lines.get(0));
    assertEquals("\"doc2\",\"456\",\"def\"", lines.get(1));
  }

  @Test 
  public void testValidateConnection() {
    Map<String, Object> map = new HashMap<>();
    map.put("csv.includeHeader", true);

    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.parseMap(map);

    CSVIndexer indexer =  new CSVIndexer(config, messenger, writer, bypass, metricsPrefix)
  }
}
