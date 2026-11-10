package com.kmwllc.lucille.salesforce.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient.BinaryBody;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import org.junit.Test;

public class FetchSalesforceFileTest {

  private static final byte[] BYTES = new byte[] {1, 2, 3, 4};

  @Test
  public void testAttachmentBytesNameAndContentType() throws Exception {
    Document doc = file("00P000000000001AAA", "Attachment");
    doc.setField("Name", "notes.docx");
    doc.setField("ContentType", "application/vnd.openxmlformats-officedocument.wordprocessingml.document");
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertArrayEquals(BYTES, doc.getBytes("file_content"));
    assertEquals("notes.docx", doc.getString("file_name"));
    assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document", doc.getString("content_type"));
    assertEquals("Attachment", files.sobject);
    assertEquals("00P000000000001AAA", files.id);
    assertEquals("Body", files.blobField);
    assertEquals(FetchSalesforceFile.DEFAULT_MAX_BYTES, files.maxBytes);
  }

  @Test
  public void testContentDocumentUsesLatestVersion() throws Exception {
    Document doc = file("069000000000001AAA", "ContentDocument");
    doc.setField("LatestPublishedVersionId", "068000000000001AAA");
    doc.setField("Title", "Budget");
    doc.setField("FileExtension", "xlsx");
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertArrayEquals(BYTES, doc.getBytes("file_content"));
    assertEquals("Budget.xlsx", doc.getString("file_name"));
    assertEquals("ContentVersion", files.sobject);
    assertEquals("068000000000001AAA", files.id);
    assertEquals("VersionData", files.blobField);
  }

  @Test
  public void testContentVersionUsesPathAndResponseType() throws Exception {
    Document doc = file("068000000000002AAA", "contentversion");
    doc.setField("PathOnClient", "C:\\docs\\plan.pdf");
    doc.setField("Title", "ignored");
    FakeFiles files = new FakeFiles();
    files.body = new BinaryBody(BYTES, "application/pdf");

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertEquals("plan.pdf", doc.getString("file_name"));
    assertEquals("application/pdf", doc.getString("content_type"));
    assertEquals("ContentVersion", files.sobject);
    assertEquals("068000000000002AAA", files.id);
    assertEquals("VersionData", files.blobField);
  }

  @Test
  public void testDocumentBodyAndExistingExtension() throws Exception {
    Document doc = file("015000000000001AAA", "Document");
    doc.setField("Name", "policy.pdf");
    FakeFiles files = new FakeFiles();

    FetchSalesforceFile stage = new FetchSalesforceFile(config("objectType: \"document\""), files);
    doc.setField("salesforce_sobject", "Account");
    stage.processDocument(doc);

    assertEquals("Document", files.sobject);
    assertEquals("Body", files.blobField);
    assertEquals("policy.pdf", doc.getString("file_name"));
    stage.stop();
    assertTrue(files.closed);
  }

  @Test
  public void testOtherObjectsAreLeftUnchanged() throws Exception {
    Document doc = file("001000000000001AAA", "Account");
    doc.setField("Name", "Acme");
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertFalse(doc.has("file_content"));
    assertFalse(doc.has("file_name"));
    assertEquals(0, files.downloads);
  }

  @Test
  public void testMissingVersionIdSkipsTheDownload() throws Exception {
    Document doc = file("069000000000009AAA", "ContentDocument");
    doc.setField("Title", "Orphan");
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertFalse(doc.has("file_content"));
    assertEquals("Orphan", doc.getString("file_name"));
    assertEquals(0, files.downloads);
  }

  @Test
  public void testExistingBytesAreNotDownloadedAgain() throws Exception {
    Document doc = file("00P000000000002AAA", "Attachment");
    doc.setField("Name", "already.bin");
    doc.setField("file_content", new byte[] {9});
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertArrayEquals(new byte[] {9}, doc.getBytes("file_content"));
    assertEquals("already.bin", doc.getString("file_name"));
    assertEquals(0, files.downloads);
  }

  @Test
  public void testDownloadFailureFailsTheDocument() throws Exception {
    Document doc = file("00P000000000003AAA", "Attachment");
    doc.setField("Name", "missing.docx");
    FakeFiles files = new FakeFiles();
    files.failure = new IOException("HTTP 404: NOT_FOUND");

    StageException error = assertThrows(StageException.class,
        () -> new FetchSalesforceFile(config(""), files).processDocument(doc));

    assertTrue(error.getMessage().contains("HTTP 404"));
    assertFalse(doc.has("file_content"));
  }

  @Test
  public void testTitleKeepsItsExtension() throws Exception {
    Document doc = file("069000000000003AAA", "ContentDocument");
    doc.setField("LatestPublishedVersionId", "068000000000003AAA");
    doc.setField("Title", "Budget.XLSX");
    doc.setField("FileExtension", "xlsx");
    FakeFiles files = new FakeFiles();

    new FetchSalesforceFile(config(""), files).processDocument(doc);

    assertEquals("Budget.XLSX", doc.getString("file_name"));
  }

  @Test
  public void testRejectsUnknownTypeMissingAuthAndHugeLimit() throws Exception {
    assertThrows(IllegalArgumentException.class,
        () -> new FetchSalesforceFile(config("objectType: \"Account\"")));
    IllegalArgumentException auth = assertThrows(IllegalArgumentException.class, () -> new FetchSalesforceFile(config("")));
    assertTrue(auth.getMessage().contains("accessToken"));
    assertThrows(IllegalArgumentException.class, () -> new FetchSalesforceFile(config("""
        maxBytes: 3000000000
        accessToken: "t"
        instanceUrl: "https://example.my.salesforce.com"
        """)));
    new FetchSalesforceFile(config("fileNameField: \"\""), new FakeFiles());
  }

  private static Document file(String id, String type) {
    Document doc = Document.create(id);
    doc.setField("Id", id);
    doc.setField("salesforce_sobject", type);
    return doc;
  }

  private static Config config(String extra) {
    return ConfigFactory.parseString("""
        name: "fetch-file"
        class: "com.kmwllc.lucille.salesforce.stage.FetchSalesforceFile"
        """ + extra);
  }

  private static final class FakeFiles implements SalesforceBlobClient {
    private String sobject;
    private String id;
    private String blobField;
    private long maxBytes;
    private int downloads;
    private boolean closed;
    private IOException failure;
    private BinaryBody body = new BinaryBody(BYTES, "application/octet-stream");

    @Override
    public void connect() {
    }

    @Override
    public BinaryBody downloadBlob(String sobject, String id, String blobField, long maxBytes) throws IOException {
      downloads++;
      this.sobject = sobject;
      this.id = id;
      this.blobField = blobField;
      this.maxBytes = maxBytes;
      if (failure != null) {
        throw failure;
      }
      return body;
    }

    @Override
    public void close() {
      closed = true;
    }
  }
}
