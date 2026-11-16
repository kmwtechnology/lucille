package com.kmwllc.lucille.salesforce.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient.BinaryBody;
import com.kmwllc.lucille.salesforce.connector.SalesforceSessions;
import com.typesafe.config.Config;
import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Downloads the bytes of a Salesforce file onto a Document so TextExtractor can parse them.
 *
 * <p>The Salesforce connector publishes Attachment, ContentDocument, ContentVersion, and Document records without their
 * file bytes. This stage reads {@code sobjectField} (the connector's {@code salesforce_sobject} by default) and
 * downloads the blob:
 * <ul>
 *   <li>Attachment and Document: {@code Body} for {@code Id}.</li>
 *   <li>ContentVersion: {@code VersionData} for {@code Id}.</li>
 *   <li>ContentDocument: {@code VersionData} for {@code versionIdField} (default {@code LatestPublishedVersionId}).</li>
 * </ul>
 * Any other object is left unchanged, so the stage can sit on a pipeline that also carries Accounts and Contacts.
 * A file record that is missing its id, or a ContentDocument missing its version id, is left unchanged and a warning
 * is logged. A download that fails marks that document failed.
 *
 * <p>The bytes are stored in {@code byteArrayField} (default {@code file_content}), which is the field TextExtractor's
 * {@code byteArrayField} should name. The file name is copied to {@code fileNameField} (default {@code file_name}) from
 * {@code Name}, {@code PathOnClient}, or {@code Title} plus {@code FileExtension}. The content type is copied to
 * {@code contentTypeField} (default {@code content_type}) from the record's {@code ContentType} when it has one, and
 * otherwise from the download response. Point TextExtractor's {@code metadataHintFields} at those two fields
 * ({@code resourceName} and {@code Content-Type}) so Word, Excel, and the other uploaded types are detected.
 * Existing values in those fields are left as they are, and a document that already has {@code byteArrayField} is not
 * downloaded again.
 *
 * <p>Authentication is the same as the connector: {@code accessToken} and {@code instanceUrl}, or {@code clientId} and
 * {@code clientSecret} with the username-password or client-credentials flow. The stage has its own session.
 *
 * <p>Config Parameters:
 * <ul>
 *   <li>accessToken (String, Optional): Session token. When set, {@code instanceUrl} is required and no login is performed.</li>
 *   <li>instanceUrl (String, Optional): Instance the token belongs to, such as {@code https://mycompany.my.salesforce.com}.</li>
 *   <li>clientId (String, Optional): Connected-app consumer key. Required unless {@code accessToken} is set.</li>
 *   <li>clientSecret (String, Optional): Connected-app consumer secret. Required unless {@code accessToken} is set.</li>
 *   <li>username (String, Optional): Username for the username-password flow. When set, {@code password} is required.
 *       When omitted, the client-credentials flow is used.</li>
 *   <li>password (String, Optional): Password for the username-password flow, with the security token appended when the
 *       org requires one.</li>
 *   <li>loginUrl (String, Optional): Token endpoint host. Defaults to {@code https://login.salesforce.com}. Use
 *       {@code https://test.salesforce.com} for a sandbox. The client-credentials flow needs the org's My Domain URL.</li>
 *   <li>apiVersion (String, Optional): REST API version, such as {@code 67.0} or {@code v67.0}. Defaults to 67.0.</li>
 *   <li>sobjectField (String, Optional): Field holding the object API name. Defaults to {@code salesforce_sobject}.</li>
 *   <li>objectType (String, Optional): Download every document as this object type instead of reading {@code sobjectField}.
 *       One of Attachment, ContentDocument, ContentVersion, Document.</li>
 *   <li>idField (String, Optional): Field holding the record id. Defaults to {@code Id}.</li>
 *   <li>versionIdField (String, Optional): ContentVersion id on a ContentDocument. Defaults to {@code LatestPublishedVersionId}.</li>
 *   <li>byteArrayField (String, Optional): Field the file bytes are written to. Defaults to {@code file_content}.</li>
 *   <li>fileNameField (String, Optional): Field the file name is written to. Defaults to {@code file_name}. A blank value disables it.</li>
 *   <li>contentTypeField (String, Optional): Field the content type is written to. Defaults to {@code content_type}.
 *       A blank value disables it.</li>
 *   <li>maxBytes (Long, Optional): Largest file that will be downloaded. A larger file fails that document.
 *       Defaults to 104857600 (100 MiB). Must be positive and at most 2147483639.</li>
 *   <li>requestTimeoutMs (Integer, Optional): Per-request timeout in milliseconds. Defaults to 120000.</li>
 *   <li>maxRetries (Integer, Optional): Retries after a timeout, a connection failure, or HTTP 408, 429, or 5xx.
 *       Defaults to 3. A 401 on a token this client requested is refreshed once, and that refresh is not a retry.</li>
 * </ul>
 */
public class FetchSalesforceFile extends Stage {

  private static final Logger log = LoggerFactory.getLogger(FetchSalesforceFile.class);

  /** Largest file downloaded when {@code maxBytes} is omitted: 100 MiB. */
  static final long DEFAULT_MAX_BYTES = 100L * 1024 * 1024;

  /** A byte array cannot be larger than this. */
  private static final long MAX_BYTES_LIMIT = Integer.MAX_VALUE - 8L;

  private static final Map<String, String> FILE_OBJECTS = Map.of(
      "attachment", "Attachment",
      "contentdocument", "ContentDocument",
      "contentversion", "ContentVersion",
      "document", "Document");

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("accessToken", "instanceUrl", "clientId", "clientSecret", "username", "password", "loginUrl",
          "apiVersion", "sobjectField", "objectType", "idField", "versionIdField", "byteArrayField", "fileNameField",
          "contentTypeField")
      .optionalNumber("maxBytes", "requestTimeoutMs", "maxRetries")
      .build();

  private final String objectType;
  private final String sobjectField;
  private final String idField;
  private final String versionIdField;
  private final String byteArrayField;
  private final String fileNameField;
  private final String contentTypeField;
  private final long maxBytes;
  private final SalesforceBlobClient client;
  private boolean connected;

  public FetchSalesforceFile(Config config) {
    this(config, null);
  }

  /** Supplies the client. Tests use this to avoid the network. */
  FetchSalesforceFile(Config config, SalesforceBlobClient client) {
    super(config);

    String configuredType = blankToNull(ConfigUtils.getStringOrDefault(config, "objectType", null));
    if (configuredType == null) {
      this.objectType = null;
    } else {
      this.objectType = FILE_OBJECTS.get(configuredType.toLowerCase(Locale.ROOT));
      if (this.objectType == null) {
        throw new IllegalArgumentException(
            "objectType must be Attachment, ContentDocument, ContentVersion, or Document, got \"" + configuredType + "\".");
      }
    }
    this.sobjectField = blankToNull(ConfigUtils.getStringOrDefault(config, "sobjectField", "salesforce_sobject"));
    if (objectType == null && sobjectField == null) {
      throw new IllegalArgumentException("Set objectType, or set sobjectField to the field that holds the object API name.");
    }
    this.idField = requireField("idField", ConfigUtils.getStringOrDefault(config, "idField", "Id"));
    this.versionIdField = requireField("versionIdField",
        ConfigUtils.getStringOrDefault(config, "versionIdField", "LatestPublishedVersionId"));
    this.byteArrayField = requireField("byteArrayField",
        ConfigUtils.getStringOrDefault(config, "byteArrayField", "file_content"));
    this.fileNameField = optionalField("fileNameField", ConfigUtils.getStringOrDefault(config, "fileNameField", "file_name"));
    this.contentTypeField = optionalField("contentTypeField",
        ConfigUtils.getStringOrDefault(config, "contentTypeField", "content_type"));
    rejectSame(byteArrayField, fileNameField, "byteArrayField", "fileNameField");
    rejectSame(byteArrayField, contentTypeField, "byteArrayField", "contentTypeField");
    rejectSame(fileNameField, contentTypeField, "fileNameField", "contentTypeField");

    Long configuredMax = ConfigUtils.getLongOrDefault(config, "maxBytes", DEFAULT_MAX_BYTES);
    if (configuredMax == null || configuredMax < 1 || configuredMax > MAX_BYTES_LIMIT) {
      throw new IllegalArgumentException(
          "maxBytes must be from 1 to " + MAX_BYTES_LIMIT + ", got " + configuredMax + ".");
    }
    this.maxBytes = configuredMax;
    this.client = client != null ? client : SalesforceSessions.open(config);
  }

  @Override
  public void start() throws StageException {
    connect();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String type = objectType != null ? objectType : canonicalType(text(doc, sobjectField));
    if (type == null) {
      return null;
    }
    writeFileName(doc, type);
    writeContentType(doc, text(doc, "ContentType"));
    if (doc.has(byteArrayField)) {
      return null;
    }

    String id = text(doc, idField);
    if (id == null) {
      log.warn("Salesforce {} document {} has no {}; leaving it unchanged.", type, doc.getId(), idField);
      return null;
    }
    String downloadId = id;
    String sobject = type;
    String blobField = "Body";
    if ("ContentDocument".equals(type)) {
      downloadId = text(doc, versionIdField);
      if (downloadId == null) {
        log.warn("Salesforce ContentDocument {} has no {}; leaving it unchanged. Include {} in the crawl.",
            doc.getId(), versionIdField, versionIdField);
        return null;
      }
      sobject = "ContentVersion";
      blobField = "VersionData";
    } else if ("ContentVersion".equals(type)) {
      blobField = "VersionData";
    }

    connect();
    BinaryBody body;
    try {
      body = client.downloadBlob(sobject, downloadId, blobField, maxBytes);
    } catch (IOException e) {
      throw new StageException("Unable to download " + type + " file for " + doc.getId() + ": " + e.getMessage(), e);
    }
    doc.setField(byteArrayField, body.bytes());
    writeContentType(doc, body.contentType());
    return null;
  }

  @Override
  public void stop() throws StageException {
    if (client == null) {
      return;
    }
    try {
      client.close();
    } catch (IOException e) {
      throw new StageException("Error closing Salesforce client.", e);
    }
  }

  private void connect() throws StageException {
    if (connected) {
      return;
    }
    try {
      client.connect();
    } catch (IOException e) {
      throw new StageException("Unable to connect to Salesforce: " + e.getMessage(), e);
    }
    connected = true;
  }

  private void writeFileName(Document doc, String type) {
    if (fileNameField == null || doc.has(fileNameField)) {
      return;
    }
    String name = fileName(doc, type);
    if (name != null) {
      doc.setField(fileNameField, name);
    }
  }

  private void writeContentType(Document doc, String contentType) {
    if (contentTypeField == null || contentType == null || contentType.isBlank() || doc.has(contentTypeField)) {
      return;
    }
    doc.setField(contentTypeField, contentType);
  }

  private static String fileName(Document doc, String type) {
    return switch (type) {
      case "Attachment", "Document" -> text(doc, "Name");
      case "ContentVersion" -> {
        String path = text(doc, "PathOnClient");
        yield path != null ? lastSegment(path) : withExtension(text(doc, "Title"), text(doc, "FileExtension"));
      }
      case "ContentDocument" -> withExtension(text(doc, "Title"), text(doc, "FileExtension"));
      default -> null;
    };
  }

  private static String withExtension(String title, String extension) {
    if (title == null) {
      return null;
    }
    if (extension == null || extension.isBlank()) {
      return title;
    }
    String ext = extension.startsWith(".") ? extension.substring(1) : extension;
    if (ext.isBlank()) {
      return title;
    }
    if (title.toLowerCase(Locale.ROOT).endsWith("." + ext.toLowerCase(Locale.ROOT))) {
      return title;
    }
    return title + "." + ext;
  }

  private static String lastSegment(String path) {
    int slash = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
    String name = slash >= 0 ? path.substring(slash + 1) : path;
    return name.isBlank() ? null : name;
  }

  private static String canonicalType(String raw) {
    if (raw == null) {
      return null;
    }
    return FILE_OBJECTS.get(raw.toLowerCase(Locale.ROOT));
  }

  private static String text(Document doc, String field) {
    if (field == null || !doc.has(field)) {
      return null;
    }
    String value = doc.getString(field);
    if (value == null || value.isBlank()) {
      return null;
    }
    return value.trim();
  }

  private static String requireField(String name, String value) {
    String field = blankToNull(value);
    if (field == null) {
      throw new IllegalArgumentException(name + " is blank.");
    }
    if (Document.RESERVED_FIELDS.contains(field)) {
      throw new IllegalArgumentException(name + " \"" + field + "\" is a reserved field name.");
    }
    return field;
  }

  private static String optionalField(String name, String value) {
    String field = blankToNull(value);
    if (field == null) {
      return null;
    }
    if (Document.RESERVED_FIELDS.contains(field)) {
      throw new IllegalArgumentException(name + " \"" + field + "\" is a reserved field name.");
    }
    return field;
  }

  private static void rejectSame(String left, String right, String leftName, String rightName) {
    if (left != null && left.equals(right)) {
      throw new IllegalArgumentException(leftName + " and " + rightName + " must be different fields.");
    }
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }
}
