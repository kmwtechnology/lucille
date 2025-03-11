package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.storageclient.GoogleStorageClient;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class ApplyFileHandlersTest {

  private final StageFactory factory = StageFactory.of(ApplyFileHandlers.class);

  private final Path testCsvPath = Paths.get("src/test/resources/ApplyFileHandlersTest/test.csv");
  private final Path testJsonlPath = Paths.get("src/test/resources/ApplyFileHandlersTest/test.jsonl");
  private final Path testFaultyCsvPath = Paths.get("src/test/resources/ApplyFileHandlersTest/faulty.csv");

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
  public void testNoHandlers() throws StageException {
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
  public void testInvalidConfs() throws StageException {
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/empty.conf"));
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/noHandlers.conf"));
    // Start is called in the factory
    assertThrows(StageException.class, () -> factory.get("ApplyFileHandlersTest/onlyInvalidHandlers.conf"));
  }

  @Test
  public void testApplyCloudPathsAndURIs() throws StageException, IOException {
    // checking that we can run the stage on cloud files and also URIs to local files as well.
    URI googleCsvURI = URI.create("gs://bucket/test.csv");
    URI googleJsonlURI = URI.create("gs://bucket/test.jsonl");

    URI localCsvURI = URI.create(testCsvPath.toString());
    URI localJsonlURI = URI.create(testJsonlPath.toString());

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
}
