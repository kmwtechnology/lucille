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
import java.io.File;
import java.io.FileInputStream;
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
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/semicolons.csv";
    File file = new File(filePath);
    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
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
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/bom.csv";
    File file = new File(filePath);
    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
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
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/tabs.csv";
    File file = new File(filePath);
    Iterator<Document> docs = handler.processFile(new FileInputStream(file), filePath);
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

    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv";
    File file = new File(filePath);
    Iterator<Document> docs = csvFileHandler.processFile(new FileInputStream(file), filePath);

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

    File file = new File("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

    Iterator<Document> docs;

    try (FileInputStream fis = new FileInputStream(file)) {
      docs = csvFileHandler.processFile(fis, "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

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
  }

  @Test
  public void testDocIdPrefix() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("docIdPrefix", "csv_")));
    FileHandler csvFileHandler = FileHandler.create("csv", config);

    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv";
    File file = new File(filePath);
    Iterator<Document> docs = csvFileHandler.processFile(new FileInputStream(file), filePath);

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
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv";
    File file = new File(filePath);
    FileInputStream stream = new FileInputStream(file);

    // adding a null Document to the iterator to check that we do not publish it
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2"), null, Document.create("3"));
    when(spyCsvHandler.processFile(stream, filePath)).thenReturn(docs);
    doReturn(docs).when(spyCsvHandler).processFile(any(), any());
    spyCsvHandler.processFileAndPublish(publisher, stream, filePath);

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
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv";
    File file = new File(filePath);
    FileInputStream stream = new FileInputStream(file);

    // adding an exception when calling next()
    // note for this test we are arbitrarily creating fake documents with ids 1, 2, 3. We just want to test that document id 3
    // is published even though we encountered an error while iterating through the iterator
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next()).thenReturn(Document.create("1"), Document.create("2")).thenThrow(new RuntimeException("Iterator failed")).thenReturn(Document.create("3"));
    when(spyCsvHandler.processFile(stream, filePath)).thenReturn(docs);
    doReturn(docs).when(spyCsvHandler).processFile(any(), any());
    spyCsvHandler.processFileAndPublish(publisher, stream, filePath);

    List<Document> docsPublished = messenger.getDocsSentForProcessing();

    // check that we published all documents even with Exception thrown while iterating through iterator
    assertEquals(3, docsPublished.size());

    // rest of the documents were published
    assertEquals("1", docsPublished.get(0).getId());
    assertEquals("2", docsPublished.get(1).getId());
    assertEquals("3", docsPublished.get(2).getId());
  }

  @Test
  public void testCustomLineNumberField() throws Exception {
    // just making sure we can change the line number field.
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/customLineNumberField.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "defaults.csv");

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;

      assertEquals(Integer.valueOf(docsCount), d.getInt("line_number"));
      assertFalse(d.has("csvLineNumber"));
    }
  }

  @Test
  public void testCustomFilenameField() throws Exception {
    // just making sure we can change the filename field.
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/customFilenameField.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "defaults.csv");

    while (docs.hasNext()) {
      Document d = docs.next();

      assertEquals("defaults.csv", d.getString("file_name_here"));
      assertFalse(d.has("filename"));
    }
  }

  @Test
  public void testCustomFilepathField() throws Exception {
    // just making sure we can change the filePath field.
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/customFilepathField.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

    while (docs.hasNext()) {
      Document d = docs.next();

      // will just put whatever is provided to the method call.
      assertEquals("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv", d.getString("path_to_file"));
      assertFalse(d.has("source"));
    }
  }

  @Test
  public void testCustomDocIDFormat() throws Exception {
    // the format separates the three strings with an underscore
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/customDocIDFormat.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

    Document d = docs.next();
    assertEquals("a_b_c", d.getId());

    // looks a bit wonky. "e,f" is a literal string in the csv.
    d = docs.next();
    assertEquals("d_e,f_g", d.getId());

    d = docs.next();
    assertEquals("x_y_z", d.getId());
  }

  @Test
  public void testLowercaseFields() throws Exception {
    // the header columns are all lowercased. they have different capitalizations in the config
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/lowercaseFields.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

    // even though they were capitalized differently in the config, should be extracted the same as before because lowercaseFields = true
    Document d = docs.next();
    assertEquals("a_b_c", d.getId());

    d = docs.next();
    assertEquals("d_e,f_g", d.getId());

    d = docs.next();
    assertEquals("x_y_z", d.getId());
  }

  @Test
  public void testIgnoreTerms() throws Exception {
    // ignored terms is set to exclude all of the values found in "field2".
    Config config = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/CSVFileHandlerTest/config/ignoredTerms.conf");
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "defaults.csv");

    while (docs.hasNext()) {
      Document d = docs.next();

      assertTrue(d.has("field1"));
      // all the values are in "ignoredTerms" so it never gets placed on a document
      assertFalse(d.has("field2"));
      assertTrue(d.has("field3"));
    }
  }

  // empty and blank lines from the reader (iterator), make sure they get skipped

  // skipping a row with more columns than the header
  @Test
  public void testSkipRowsWithExtraColumns() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaultsExtraTerms.csv"), "defaults.csv");

    int docsCount = 0;

    while (docs.hasNext()) {
      docs.next();
      docsCount++;
    }

    // the second line should've been skipped
    assertEquals(2, docsCount);
  }

  // null header (empty file) --> handled gracefully, no documents to be published?
  @Test
  public void testEmptyFile() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/empty.csv"), "defaults.csv");
    assertFalse(docs.hasNext());
  }

  @Test
  public void testHeaderNoRows() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/headerOnly.csv"), "defaults.csv");
    assertFalse(docs.hasNext());
  }

  @Test
  public void testBlanksAndEmpties() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaultsWithEmptiesAndBlanks.csv"), "defaults.csv");

    int docsCount = 0;

    while (docs.hasNext()) {
      docsCount++;
      Document d = docs.next();
      // 1, 4, and 7 are the line numbers. (still get counted even with the blank lines!)
      assertEquals(Integer.valueOf((3 * (docsCount - 1)) + 1), d.getInt("csvLineNumber"));
    }

    // still should just be the three docs
    assertEquals(3, docsCount);
  }

  // file handler should safely ignore reserved fields when there is a column with the same name in the header
  @Test
  public void testIgnoreReservedFields() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/reservedFields.csv"), "defaults.csv");

    int docsCount = 0;
    while (docs.hasNext()) {

      Document d = docs.next();
      docsCount++;

      assertEquals("defaults.csv-" + docsCount, d.getId());
      // was in the headers but shouldn't be placed on document
      assertFalse(d.has(Document.RUNID_FIELD));
      // csv has 1, 2, 3 for "value" :)
      assertEquals(Integer.valueOf(docsCount), d.getInt("value"));
    }
  }

  @Test
  public void testExhaustingIterator() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"), "defaults.csv");

    while (docs.hasNext()) {
      docs.next();
    }

    // an exception is thrown but caught, so just an error is logged
    docs.next();
  }
}
