package com.kmwllc.lucille.core.fileHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class CSVFileHandlerTest {
  @Test
  public void testSemicolonSeparator() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("separatorChar",  ";")));
    FileHandler handler = FileHandler.create("csv", config);

    // contents of CSVConnectorTest/semicolons.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	f	g
    //  x	y	z

    // verify number of documents and exhaust iterator to close resources
    Iterator<Document> docs = handler.processFile(Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/semicolons.csv"));
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());
    assertEquals("a", doc1.getString("field1"));
  }

  @Test
  public void testBOMHandling() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    FileHandler handler = FileHandler.create("csv", config);

    // contents of bom.csv (first character is the BOM character \uFEFF)
    // name, price, country
    // Carbonara, 30, Italy
    // Pizza, 10, Italy
    // Tofu Soup, 12, Korea

    // verify number of documents and exhaust iterator to close resources
    Iterator<Document> docs = handler.processFile(Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/bom.csv"));
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    assertTrue(doc1.getFieldNames().contains("name"));

    // there should be no issues accessing the field value of the first column because BOM is removed
    assertEquals("Carbonara", doc1.getString("name"));
  }

  @Test
  public void testTabsAndNoninterpretedQuotes() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of(
        "interpretQuotes", false,
        "useTabs", true,
        "separatorChar", ";"
    )));
    FileHandler handler = FileHandler.create("csv", config);

    // contents of CSVConnectorTest/config.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	"e,f	g
    //  x	y	z

    // verify number of documents and exhaust iterator to close resources
    Iterator<Document> docs = handler.processFile(Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/tabs.tsv"));
    // verify number of documents and exhaust iterator to close resources
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    // verify that tabs takes precedence over specified separator character
    assertEquals("\"e,f", doc2.getString("field2"));
  }

  @Test
  public void testProcessFilePathDefaults() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    FileHandler csvFileHandler = FileHandler.create("csv", config);

    Path path = Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv");
    Iterator<Document> docs = csvFileHandler.processFile(path);

    // contents of CSVConnectorTest/config.conf
    // field1, field2, field3
    //  a, b, c
    //  d, "e,f", g
    //  x, y, z

    // verify number of documents and exhaust iterator to close resources
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    assertEquals("a", doc1.getString("field1"));
    assertEquals("b", doc1.getString("field2"));
    assertEquals("c", doc1.getString("field3"));

    assertEquals("d", doc2.getString("field1"));
    assertEquals("e,f", doc2.getString("field2"));
    assertEquals("g", doc2.getString("field3"));

    assertEquals("x", doc3.getString("field1"));
    assertEquals("y", doc3.getString("field2"));
    assertEquals("z", doc3.getString("field3"));
  }

  @Test
  public void testProcessFileBytesDefaults() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    FileHandler csvFileHandler = FileHandler.create("csv", config);

    byte[] fileBytes = Files.readAllBytes(Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv"));
    Iterator<Document> docs = csvFileHandler.processFile(fileBytes, "src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv");

    // contents of CSVConnectorTest/config.conf
    // field1, field2, field3
    //  a, b, c
    //  d, "e,f", g
    //  x, y, z

    // verify number of documents and exhaust iterator to close resources
    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    assertEquals("a", doc1.getString("field1"));
    assertEquals("b", doc1.getString("field2"));
    assertEquals("c", doc1.getString("field3"));

    assertEquals("d", doc2.getString("field1"));
    assertEquals("e,f", doc2.getString("field2"));
    assertEquals("g", doc2.getString("field3"));

    assertEquals("x", doc3.getString("field1"));
    assertEquals("y", doc3.getString("field2"));
    assertEquals("z", doc3.getString("field3"));
  }

  @Test
  public void testDocIdPrefix() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("docIdPrefix", "csv_")));
    FileHandler csvFileHandler = FileHandler.create("csv", config);

    Path path = Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv");
    Iterator<Document> docs = csvFileHandler.processFile(path);


    Document doc1 = docs.next();
    Document doc2 = docs.next();
    Document doc3 = docs.next();
    assertFalse(docs.hasNext());

    assertEquals("csv_defaults.csv-1", doc1.getId());
    assertEquals("csv_defaults.csv-2", doc2.getId());
    assertEquals("csv_defaults.csv-3", doc3.getId());
  }

  @Test
  public void testProcessFileAndPublishSuccessful() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    CSVFileHandler spyCsvHandler = (CSVFileHandler) spy(FileHandler.create("csv", config));
    Path path = Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv");

    // adding a null Document to the iterator to check that we do not publish it
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2"), null, Document.create("3"));
    when(spyCsvHandler.processFile(path)).thenReturn(docs);
    doReturn(docs).when(spyCsvHandler).processFile(any());
    spyCsvHandler.processFileAndPublish(publisher, path);

    List<Document> docsPublished = messenger.getDocsSentForProcessing();

    // null document was ignored
    assertEquals(3, docsPublished.size());

    // rest of the documents were published
    assertEquals("1", docsPublished.get(0).getId());
    assertEquals("2", docsPublished.get(1).getId());
    assertEquals("3", docsPublished.get(2).getId());
  }

  @Test
  public void testProcessFileWithIteratorExhaustionFailure() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    CSVFileHandler spyCsvHandler = (CSVFileHandler) spy(FileHandler.create("csv", config));
    Path path = Paths.get("src/test/resources/FileHandlerTest/CSVFileHandler/defaults.csv");

    // adding an exception when calling next()
    // note for this test we are arbitrarily creating fake documents with ids 1, 2, 3. We just want to test that document id 3
    // is published even though we encountered an error while iterating through the iterator
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2")).thenThrow(new RuntimeException("Iterator failed")).thenReturn(Document.create("3"));
    when(spyCsvHandler.processFile(path)).thenReturn(docs);
    doReturn(docs).when(spyCsvHandler).processFile(any());
    spyCsvHandler.processFileAndPublish(publisher, path);

    List<Document> docsPublished = messenger.getDocsSentForProcessing();

    // check that we published all documents even with Exception thrown while iterating through iterator
    assertEquals(3, docsPublished.size());

    // rest of the documents were published
    assertEquals("1", docsPublished.get(0).getId());
    assertEquals("2", docsPublished.get(1).getId());
    assertEquals("3", docsPublished.get(2).getId());
  }

}
