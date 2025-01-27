package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.TestMessenger;
import com.opencsv.ICSVWriter;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CSVIndexerTest {

  private final File outputFile = new File("output.csv");
  private final File parentDirFile = new File("my_files");
  private final File nestedOutputFile = new File("my_files/output.csv");

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
  public void testInvalidConfig() {
    // cannot use the indexOverrideField
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/invalidConfig.conf");

    assertThrows(IllegalArgumentException.class,
        () -> new CSVIndexer(config, messenger, false, "testing"));
  }

  @Test
  public void testValidateConnection() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/includeHeader.conf");

    CSVIndexer noErrorIndexer = new CSVIndexer(config, messenger, null, false, "testing");
    assertFalse(noErrorIndexer.validateConnection());

    CSVIndexer faultyIndexer = new CSVIndexer(config, messenger, null, true, "testing");
    assertThrows(NullPointerException.class,
        () -> faultyIndexer.validateConnection());

    CSVIndexer indexer = new CSVIndexer(config, messenger, false, "testing");
    assertTrue(indexer.validateConnection());

    // no include header
    config = ConfigFactory.load("CSVIndexerTest/config.conf");
    indexer = new CSVIndexer(config, messenger, false, "testing");
    assertTrue(indexer.validateConnection());
  }

  // Ensures the CSVIndexer is willing to create directories for output files.
  @Test
  public void testMakeParentDir() {
    try {
      nestedOutputFile.delete();
      parentDirFile.delete();

      assertFalse(parentDirFile.exists());

      TestMessenger messenger = new TestMessenger();
      Config config = ConfigFactory.load("CSVIndexerTest/missingParentConfig.conf");
      new CSVIndexer(config, messenger, false, "testing");

      assertTrue(parentDirFile.exists());
    } finally {
      nestedOutputFile.delete();
      parentDirFile.delete();
    }
  }

  @Test
  public void testCloseNullWriter() {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/config.conf");
    CSVIndexer indexer = new CSVIndexer(config, messenger, null, false, "testing");

    // should not throw an error.
    indexer.closeConnection();
  }

  @Test
  public void testCloseConnectionWithError() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("CSVIndexerTest/config.conf");

    ICSVWriter mockWriter = Mockito.mock(ICSVWriter.class);
    Mockito.doThrow(new IOException("Mocked Error")).when(mockWriter).close();
    CSVIndexer indexer = new CSVIndexer(config, messenger, mockWriter, false, "testing");

    // Again, doesn't throw an error.
    indexer.closeConnection();
  }
}
