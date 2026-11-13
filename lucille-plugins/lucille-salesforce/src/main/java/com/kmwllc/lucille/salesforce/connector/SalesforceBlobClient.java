package com.kmwllc.lucille.salesforce.connector;

import java.io.IOException;

/**
 * Downloads the bytes of a Salesforce file. {@link SalesforceRestClient} calls the REST API; tests supply a fake.
 */
public interface SalesforceBlobClient extends AutoCloseable {

  /** Authorizes the client. Later calls fail when this has not succeeded. */
  void connect() throws IOException;

  /**
   * Downloads one binary field. {@code blobField} is {@code Body} for Attachment and Document, and {@code VersionData}
   * for ContentVersion. Redirects are followed and the session token is sent on each hop, because Salesforce often
   * redirects the download to a file host.
   */
  BinaryBody downloadBlob(String sobject, String id, String blobField, long maxBytes) throws IOException;

  @Override
  void close() throws IOException;

  /** The downloaded bytes and the response content type, with any charset parameter removed. */
  record BinaryBody(byte[] bytes, String contentType) {
  }
}
