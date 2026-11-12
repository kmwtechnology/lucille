package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclient.GoogleStorageClient;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class ApplyFileHandlersTest {

  private final StageFactory factory = StageFactory.of(ApplyFileHandlers.class);

  private final Path testCsvPath = Paths.get("src/test/resources/ApplyFileHandlersTest/test.csv");
  private final Path testJsonlPath = Paths.get("src/test/resources/ApplyFileHandlersTest/test.jsonl");
  private final Path testFaultyCsvPath = Paths.get("src/test/resources/ApplyFileHandlersTest/faulty.csv");
  private final Path secretCSVTXTPath = Paths.get("src/test/resources/ApplyFileHandlersTest/secretCSV.txt");

  private final byte[] testCsvContents;

  // IOException so we can read all bytes above.
  public ApplyFileHandlersTest() throws IOException {
    testCsvContents = Files.readAllBytes(testCsvPath);
  }

  @Test
  public void testApplyFileHandlersCSV() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("file_path", testCsvPath.toString());

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;

      assertEquals("test.csv", d.getString("filename"));
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testApplyFileHandlersJSON() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("json_doc");
    csvDoc.setField("file_path", testJsonlPath.toString());

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testUseFileContentField() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_contents_doc");
    // makes sure we aren't actually running onto the file system here, and instead just looking at the extension
    // and using the handler.
    csvDoc.setField("file_path", "blahblahblah.csv");
    csvDoc.setField("file_content", testCsvContents);

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testFileContentsNoPath() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_contents_doc");
    csvDoc.setField("file_content", testCsvContents);

    assertNull(stage.processDocument(csvDoc));
  }

  @Test
  public void testAlternateFileContentPath() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/alternateFileContentPath.conf");

    Document csvDoc = Document.create("csv_contents_doc");
    csvDoc.setField("file_path", "blahblahblah.csv");
    csvDoc.setField("alternate_content", testCsvContents);

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  // Want to make sure that the presence of bytes doesn't somehow cause types w/o a handler to somehow be processed...
  @Test
  public void testFileContentAndUnsupportedType() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/jsonOnly.conf");

    Document csvDoc = Document.create("csv_contents_doc");
    csvDoc.setField("file_path", testCsvPath.toString());
    csvDoc.setField("alternate_content", testCsvContents);

    assertNull(stage.processDocument(csvDoc));
  }

  @Test
  public void testFileContentWithoutCloudConfig() throws StageException {
    // has no cloud config. so we can't create a file fetcher that works with s3 - and we won't need to, because
    // we have the file contents on the document already...
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_contents_doc");
    csvDoc.setField("file_path", "s3://bucket/test.csv");
    csvDoc.setField("file_content", testCsvContents);

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testAlternateFilePathField() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/specialPath.conf");

    // Going to make sure that the stage uses only the specified field ("source") and not the default, "file_path".
    // If it were to process "file_path", it would get an error from processing the faulty csv.
    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("file_path", testFaultyCsvPath.toString());
    csvDoc.setField("source", testCsvPath.toString());

    Iterator<Document> docs = stage.processDocument(csvDoc);

    int docsCount = 0;

    while (docs.hasNext()) {
      Document d = docs.next();
      docsCount++;
      assertEquals(docsCount + "", d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testNoFilePathField() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/specialPath.conf");

    // Similar to above, pointing this to the faulty doc, which SHOULDN'T get processed, because we have a different
    // file path field specified.
    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("file_path", testFaultyCsvPath.toString());

    assertNull(stage.processDocument(csvDoc));
  }

  @Test
  public void testNoHandlersForFileTypes() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/csvOnly.conf");
    Document jsonDoc = Document.create("json_doc");
    jsonDoc.setField("file_path", testJsonlPath.toString());
    assertNull(stage.processDocument(jsonDoc));

    stage = factory.get("ApplyFileHandlersTest/jsonOnly.conf");
    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("file_path", testCsvPath.toString());
    assertNull(stage.processDocument(csvDoc));
  }

  @Test
  public void testFaultyHandling() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document csvDoc = Document.create("csv_doc");
    csvDoc.setField("file_path", testFaultyCsvPath.toString());

    assertThrows(StageException.class, () -> stage.processDocument(csvDoc));
  }

  @Test
  public void testCustomFileHandler() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/implementedCustom.conf");

    Document doc = Document.create("doc");
    doc.setField("file_path", secretCSVTXTPath.toString());

    Iterator<Document> handledDocs = stage.processDocument(doc);

    int docsCount = 0;

    while (handledDocs.hasNext()) {
      Document d = handledDocs.next();
      docsCount++;
      assertEquals(DigestUtils.md5Hex(d.getString("file_path")) + "-" + docsCount, d.getId());
    }

    assertEquals(4, docsCount);
  }

  @Test
  public void testChildrenFollowTheFileHandlerIteratorContract() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/contractCustom.conf");

    // a record whose next() throws, and a null, are skipped
    Document doc = Document.create("doc");
    doc.setField("file_path", "records.rec");
    doc.setField("file_content", "a\nbad\nnull\nb\nnull\n".getBytes(StandardCharsets.UTF_8));

    List<String> ids = new ArrayList<>();
    stage.processDocument(doc).forEachRemaining(d -> ids.add(d.getId()));
    assertEquals(List.of("a", "b"), ids);

    // a hasNext() that throws stops the file, after the children read before it
    Document failing = Document.create("failing");
    failing.setField("file_path", "records.rec");
    failing.setField("file_content", "a\nfail\nb\n".getBytes(StandardCharsets.UTF_8));

    Iterator<Document> children = stage.processDocument(failing);
    assertEquals("a", children.next().getId());
    IllegalStateException e = assertThrows(IllegalStateException.class, children::hasNext);
    assertTrue(e.getMessage(), e.getMessage().contains("records.rec"));
  }

  /**
   * One record per line, whose id is the line: "bad" makes next() throw, "null" makes it return null, and "fail" makes
   * hasNext() throw.
   */
  public static class ContractFileHandler implements FileHandler {

    public ContractFileHandler(Config config) {
    }

    @Override
    public Iterator<Document> processFile(InputStream inputStream, String pathStr) {
      Iterator<String> lines = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8)).lines().iterator();
      return new Iterator<>() {
        private String line = null;

        @Override
        public boolean hasNext() {
          if (line == null && lines.hasNext()) {
            line = lines.next();
          }
          if ("fail".equals(line)) {
            throw new IllegalStateException("can't read on");
          }
          return line != null;
        }

        @Override
        public Document next() {
          hasNext();
          String record = line;
          line = null;
          if ("bad".equals(record)) {
            throw new IllegalArgumentException("bad record");
          }
          return "null".equals(record) ? null : Document.create(record);
        }
      };
    }
  }

  @Test
  public void testTheStreamIsClosedWhenProcessFileThrows() throws Exception {
    Stage stage = factory.get("ApplyFileHandlersTest/throwingCustom.conf");

    Document doc = Document.create("doc");
    doc.setField("file_path", secretCSVTXTPath.toString());

    assertThrows(StageException.class, () -> stage.processDocument(doc));
    // a closed FileInputStream refuses to report what is available
    assertThrows(IOException.class, () -> ThrowingFileHandler.given.available());
  }

  /** Throws from processFile, keeping the stream it was given. */
  public static class ThrowingFileHandler implements FileHandler {
    static InputStream given;

    public ThrowingFileHandler(Config config) {
    }

    @Override
    public Iterator<Document> processFile(InputStream inputStream, String pathStr) {
      given = inputStream;
      throw new IllegalStateException("can't set up");
    }
  }

  @Test
  public void testInvalidConfs() {
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/empty.conf"));
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/noHandlers.conf"));
    // attempts to use a custom file handler that is not present in the classpath
    // IllegalArgumentException is thrown in the FileHandler.create method
    assertThrows(IllegalArgumentException.class, () -> factory.get("ApplyFileHandlersTest/missingCustom.conf"));
  }

  @Test
  public void testAHandlerThatCanOnlyPublishIsRejectedAtStart() {
    StageException e = assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/xml.conf"));
    assertTrue(e.getMessage().contains("\"xml\""));
  }

  @Test
  public void testApplyCloudPathsAndURIs() throws StageException, IOException {
    // checking that we can run the stage on cloud files and also URIs to local files as well.
    URI googleCsvURI = URI.create("gs://bucket/test.csv");
    URI googleJsonlURI = URI.create("gs://bucket/test.jsonl");

    URI localCsvURI = testCsvPath.toUri();
    URI localJsonlURI = testJsonlPath.toUri();

    // for mocking - the "cloud files" will have the same contents as the test files
    InputStream csvInputStream = new FileInputStream(testCsvPath.toString());
    InputStream jsonlInputStream = new FileInputStream(testJsonlPath.toString());

    Document googleCsvDoc = Document.create("googleCsvDoc");
    googleCsvDoc.setField("file_path", googleCsvURI.toString());

    Document googleJsonlDoc = Document.create("googleJsonlDoc");
    googleJsonlDoc.setField("file_path", googleJsonlURI.toString());

    Document localCsvDoc = Document.create("localCsvDoc");
    localCsvDoc.setField("file_path", localCsvURI.toString());

    Document localJsonlDoc = Document.create("localJsonlDoc");
    localJsonlDoc.setField("file_path", localJsonlURI.toString());

    try (MockedConstruction<GoogleStorageClient> mockedConstruction = Mockito.mockConstruction(GoogleStorageClient.class, (mock, context) -> {
      when(mock.getFileContentStream(googleCsvURI)).thenReturn(csvInputStream);
      when(mock.getFileContentStream(googleJsonlURI)).thenReturn(jsonlInputStream);
    })) {
      Stage stage = factory.get("ApplyFileHandlersTest/allTypesAndCloud.conf");

      Iterator<Document> googleCsvResults = stage.processDocument(googleCsvDoc);
      Iterator<Document> googleJsonlResults = stage.processDocument(googleJsonlDoc);
      Iterator<Document> localCsvResults = stage.processDocument(localCsvDoc);
      Iterator<Document> localJsonlResults = stage.processDocument(localJsonlDoc);

      int docsCount = 0;
      while (googleCsvResults.hasNext()) {
        Document d = googleCsvResults.next();
        docsCount++;

        assertEquals("test.csv", d.getString("filename"));
      }
      assertEquals(4, docsCount);

      docsCount = 0;
      while (googleJsonlResults.hasNext()) {
        Document d = googleJsonlResults.next();
        docsCount++;

        assertEquals(docsCount + "", d.getId());
      }
      assertEquals(4, docsCount);

      docsCount = 0;
      while (localCsvResults.hasNext()) {
        Document d = localCsvResults.next();
        docsCount++;

        assertEquals("test.csv", d.getString("filename"));
      }
      assertEquals(4, docsCount);

      docsCount = 0;
      while (localJsonlResults.hasNext()) {
        Document d = localJsonlResults.next();
        docsCount++;

        assertEquals(docsCount + "", d.getId());
      }
      assertEquals(4, docsCount);
    }
  }

  @Test
  public void testURIWithoutCloudConfig() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    // Stage Exception should be thrown when trying to process a cloud document we don't have configuration for.
    Document faultyS3Doc = Document.create("s3Doc");
    faultyS3Doc.setField("file_path", URI.create("s3://bucket/test.csv").toString());
    assertThrows(StageException.class, () -> stage.processDocument(faultyS3Doc));
  }

  // Ensuring a problematic fileFetcher startup is handled gracefully.
  @Test
  public void testFailedFetcherStartup() {
    // Will try to *actually* create a GoogleStorageClient which will not succeed, as we have given it a bad
    // path to a service key.
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/allTypesAndCloud.conf"));
  }

  // an upper-case extension uses the handler configured in lower case
  @Test
  public void testUpperCaseExtension() throws StageException {
    Stage stage = factory.get("ApplyFileHandlersTest/allTypes.conf");

    Document doc = Document.create("csv_doc");
    doc.setField("file_path", "some/folder/TEST.CSV");
    doc.setField("file_content", testCsvContents);

    Iterator<Document> docs = stage.processDocument(doc);
    int docsCount = 0;
    while (docs.hasNext()) {
      docs.next();
      docsCount++;
    }

    assertEquals(4, docsCount);
  }
}
