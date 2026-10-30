package com.kmwllc.lucille.core.filehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherException;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class CSVFileHandlerTest {
  private static List<String> idsOf(FileHandler handler, String csv) throws Exception {
    Iterator<Document> docs = handler.processFile(new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8)), "inline.csv");
    List<String> ids = new ArrayList<>();
    while (docs.hasNext()) {
      Document doc = docs.next();
      if (doc != null) {
        ids.add(doc.getId());
      }
    }
    return ids;
  }

  @Test
  public void testRowsWithBlankIdsAreSkipped() throws Exception {
    // a row whose id is blank cannot be told apart from others like it, so it is skipped; the rows around it are read
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id", "docIdPrefix", "p-"))));

    String csv = "id,name\n1,a\n,b\n   ,c\n4,d\n5\n6,f\n";   // the row "5" also has the wrong number of columns

    assertEquals(List.of("p-1", "p-4", "p-6"), idsOf(handler, csv));
  }

  @Test
  public void testBlankIdRowsAtTheEndDoNotCauseAnError() throws Exception {
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id"))));

    assertEquals(List.of("1"), idsOf(handler, "id,name\n1,a\n,b\n,c\n"));
  }

  @Test
  public void testBlankIdRowsDoNotStopThePublishingOfTheRest() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id"))));

    handler.processFileAndPublish(publisher, new ByteArrayInputStream("id,name\n1,a\n,b\n3,c\n".getBytes(StandardCharsets.UTF_8)), "inline.csv");

    List<String> published = messenger.getDocsSentForProcessing().stream().map(Document::getId).toList();
    assertEquals(List.of("1", "3"), published);
  }

  @Test
  public void testIdFieldNotInHeaderFailsTheFile() throws Exception {
    // an idField that matches no column is a configuration error: every row would be unusable, so the file fails up front
    // with a message naming the column, instead of every row being skipped (or, before, dropped by a NullPointerException)
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "ID"))));

    FileHandlerException e = assertThrows(FileHandlerException.class, () -> idsOf(handler, "id,name\n1,a\n2,b\n"));
    assertTrue(e.getMessage(), e.getMessage().contains("idField 'ID'"));
    assertTrue(e.getMessage(), e.getMessage().contains("lowercaseFields"));

    // the same column name is found once the header is lower-cased
    FileHandler lowercasing = FileHandler.create("csv",
        ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "ID", "lowercaseFields", true))));
    assertEquals(List.of("1", "2"), idsOf(lowercasing, "id,name\n1,a\n2,b\n"));

    // one missing column out of several is reported by name
    FileHandler composite = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idFields", List.of("id", "nam")))));
    e = assertThrows(FileHandlerException.class, () -> idsOf(composite, "id,name\n1,a\n"));
    assertTrue(e.getMessage(), e.getMessage().contains("idField 'nam'"));

    // an empty file has no header to check against, and no rows, so it is not an error
    assertEquals(List.of(), idsOf(handler, ""));
  }

  @Test
  public void testRowsWithWrongColumnCountCountAsBadRecords() throws Exception {
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id"))));
    int limit = BadRecordTracker.MAX_CONSECUTIVE;

    // a few short or long rows are skipped and the rest of the file is read
    assertEquals(List.of("1", "3"), idsOf(handler, "id,name\n1,a\n2\n3,c\n4,d,extra\n"));

    // a header with a trailing empty cell makes every row mismatch; the file fails instead of yielding no documents
    String trailingCell = "id,name,\n" + "1,a\n".repeat(limit) + "2,b\n";
    assertThrows(BadRecordTracker.TooManyBadRecordsException.class, () -> idsOf(handler, trailingCell));

    // mismatches and blank ids count toward the same limit, and a good row resets it
    String mixed = "id,name\n" + "x\n,y\n".repeat((limit - 1) / 2) + "1,a\n" + "x\n".repeat(limit - 1) + "2,b\n";
    assertEquals(List.of("1", "2"), idsOf(handler, mixed));
  }

  @Test
  public void testCompositeIdWithSomeBlankPartsIsUsable() throws Exception {
    // only an id that is blank as a whole is unusable; one with a blank part, like "_3", is kept
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idFields", List.of("a", "b")))));

    assertEquals(List.of("1_2", "_3", "_"), idsOf(handler, "a,b,c\n1,2,x\n,3,y\n,,z\n"));
  }

  @Test
  public void testTooManyConsecutiveBlankIdRowsFailsTheFile() throws Exception {
    FileHandler handler = FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id"))));
    int limit = BadRecordTracker.MAX_CONSECUTIVE;

    // one fewer than the limit, repeatedly, is fine because a good row resets the count
    String ok = "id,name\n" + ",x\n".repeat(limit - 1) + "1,a\n" + ",x\n".repeat(limit - 1) + "2,b\n";
    assertEquals(List.of("1", "2"), idsOf(handler, ok));

    String tooMany = "id,name\n" + ",x\n".repeat(limit) + "1,a\n";
    assertThrows(BadRecordTracker.TooManyBadRecordsException.class, () -> idsOf(handler, tooMany));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");
    assertThrows(FileHandlerException.class, () -> handler.processFileAndPublish(
        publisher, new ByteArrayInputStream(tooMany.getBytes(StandardCharsets.UTF_8)), "inline.csv"));
    assertEquals(0, messenger.getDocsSentForProcessing().size());
  }

  @Test
  public void testSemicolonSeparator() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("separatorChar",  ";")));
    FileHandler handler = FileHandler.create("csv", config);

    // contents of FileHandlerTest/CSVFileHandlerTest/semicolons.csv
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

    // contents of FileHandlerTest/CSVFileHandlerTest/tabs.csv
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

    // contents of FileHandlerTest/CSVFileHandlerTest/defaults.csv
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

      // contents of FileHandlerTest/CSVFileHandlerTest/defaults.csv
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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaultsExtraTerms.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/empty.csv"),
        "defaults.csv");
    assertFalse(docs.hasNext());
  }

  @Test
  public void testHeaderNoRows() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/headerOnly.csv"),
        "defaults.csv");
    assertFalse(docs.hasNext());
  }

  @Test
  public void testBlanksAndEmpties() throws Exception {
    Config config = ConfigFactory.empty();
    CSVFileHandler handler = new CSVFileHandler(config);

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaultsWithEmptiesAndBlanks.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/reservedFields.csv"),
        "defaults.csv");

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

    Iterator<Document> docs = handler.processFile(new FileInputStream("src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv"),
        "defaults.csv");

    while (docs.hasNext()) {
      docs.next();
    }

    // an exception is thrown but caught, so just an error is logged
    docs.next();
  }

  /**
   * Test that a publish failure in processFileAndPublish() throws FileHandlerException (unrecoverable),
   * while an iterator failure (bad record) is logged and skipped, allowing subsequent documents
   * to be published.
   *
   * This verifies the separation of concerns in BaseFileHandler's processing loop: iterator
   * errors are per-record problems (skip and continue), while publish errors are framework-level
   * failures (abort immediately).
   */
  @Test
  public void testPublishFailureThrowsWhileIteratorFailureIsSkipped() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    CSVFileHandler spyCsvHandler = (CSVFileHandler) spy(FileHandler.create("csv", config));
    String filePath = "src/test/resources/FileHandlerTest/CSVFileHandlerTest/defaults.csv";
    File file = new File(filePath);
    FileInputStream stream = new FileInputStream(file);

    // Mock an iterator where: doc1 succeeds, doc2 throws from next() (iterator failure),
    // doc3 succeeds, doc4 triggers a publish failure
    Iterator<Document> docs = mock(Iterator.class);
    when(docs.hasNext()).thenReturn(true, true, true, true, false);
    when(docs.next())
        .thenReturn(Document.create("doc1"))
        .thenThrow(new RuntimeException("Bad record"))
        .thenReturn(Document.create("doc3"))
        .thenReturn(Document.create("doc4"));
    doReturn(docs).when(spyCsvHandler).processFile(any(), any());

    // Make the publisher throw on the 3rd publish call (doc4 — after doc1 and doc3 succeed)
    Publisher spyPublisher = spy(publisher);
    doCallRealMethod()
        .doCallRealMethod()
        .doThrow(new PublisherException("Simulated Kafka failure"))
        .when(spyPublisher).publish(any(Document.class));

    // processFileAndPublish should throw FileHandlerException due to the publish failure
    FileHandlerException thrown = assertThrows(FileHandlerException.class, () ->
        spyCsvHandler.processFileAndPublish(spyPublisher, stream, filePath));

    assertTrue(thrown.getMessage().contains("Error publishing file"));
    assertTrue(thrown.getCause().getMessage().contains("Simulated Kafka failure"));

    // doc1 and doc3 were published successfully (doc2 was an iterator failure, skipped)
    List<Document> published = messenger.getDocsSentForProcessing();
    assertEquals(2, published.size());
    assertEquals("doc1", published.get(0).getId());
    assertEquals("doc3", published.get(1).getId());
  }

  private static InputStream toStream(String csv) {
    return new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
  }

  @Test
  public void testMaxRecordBytes() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("maxRecordBytes", 10)));
    FileHandler handler = FileHandler.create("csv", config);

    Iterator<Document> docs = handler.processFile(toStream("id,value\n1,short\n2,this is too long\n3,short\n"), "test.csv");

    // the row before the oversized one is still returned
    assertTrue(docs.hasNext());
    assertEquals("short", docs.next().getString("value"));

    IllegalStateException e = assertThrows(IllegalStateException.class, docs::hasNext);
    assertTrue(e.getMessage().contains("Logical row 2"));
    assertTrue(e.getMessage().contains("test.csv"));
    assertFalse(docs.hasNext());
  }

  @Test
  public void testMaxRecordBytesOnHeader() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("maxRecordBytes", 5)));
    FileHandler handler = FileHandler.create("csv", config);

    assertThrows(FileHandlerException.class, () -> handler.processFile(toStream("id,value\n1,a\n"), "test.csv"));
  }

  @Test
  public void testMaxRecordBytesAcrossLines() throws Exception {
    // the second row spans three physical lines of 7, 4 and 5 bytes: each is under the limit, but the row is 16 bytes
    String csv = "id,value\n1,a\n2,\"xxxx\nyyyy\nzzzz\"\n3,b\n";

    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("maxRecordBytes", 10)));
    Iterator<Document> docs = FileHandler.create("csv", config).processFile(toStream(csv), "test.csv");
    assertEquals("a", docs.next().getString("value"));
    IllegalStateException e = assertThrows(IllegalStateException.class, docs::hasNext);
    assertTrue(e.getMessage().contains("Logical row 2"));

    // the count starts over for each row, so a limit that fits the largest row lets every row through
    config = ConfigFactory.parseMap(Map.of("csv", Map.of("maxRecordBytes", 16)));
    docs = FileHandler.create("csv", config).processFile(toStream(csv), "test.csv");
    assertEquals("a", docs.next().getString("value"));
    assertEquals("xxxx\nyyyy\nzzzz", docs.next().getString("value"));
    assertEquals("b", docs.next().getString("value"));
    assertFalse(docs.hasNext());
  }

  @Test
  public void testMaxRecordBytesStillAppliesWhileSkippingBadRows() throws Exception {
    // rows 2 and 3 are bad (blank id, wrong column count) and are skipped; row 4 is over the limit and fails the file
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("idField", "id", "maxRecordBytes", 12)));
    FileHandler handler = FileHandler.create("csv", config);

    Iterator<Document> docs = handler.processFile(toStream("id,value\n1,a\n,b\n2\n3,this value is too long\n4,c\n"), "test.csv");
    assertEquals("1", docs.next().getId());

    IllegalStateException e = assertThrows(IllegalStateException.class, docs::next);
    assertFalse(e instanceof BadRecordTracker.TooManyBadRecordsException);
    assertTrue(e.getMessage(), e.getMessage().contains("Logical row 4"));
    assertFalse(docs.hasNext());
  }

  @Test
  public void testInvalidMaxRecordBytes() {
    Config config = ConfigFactory.parseMap(Map.of("csv", Map.of("maxRecordBytes", 0)));
    assertThrows(IllegalArgumentException.class, () -> FileHandler.create("csv", config));
  }
}
