package com.kmwllc.lucille.connector.storageclient.filereference;

import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import org.apache.commons.codec.digest.DigestUtils;

/**
 * A file reference to a file on Azure.
 */
public class AzureFileReference extends BaseFileReference {
  private final BlobItem blobItem;

  public AzureFileReference(BlobItem blobItem) {
    super(blobItem.getProperties().getLastModified().toInstant());

    this.blobItem = blobItem;
  }

  @Override
  public String getFilePath(TraversalParams params) {
    URI pathURI = params.getURI();

    return String.format("%s://%s/%s/%s", pathURI.getScheme(), pathURI.getAuthority(),
        pathURI.getPath().split("/")[1], blobItem.getName());
  }

  @Override
  public boolean isCloudFileReference() {
    return true;
  }

  @Override
  public boolean validFile() {
    return !blobItem.isPrefix();
  }

  @Override
  public Document toDoc(TraversalParams params) {
    String fullPath = getFilePath(params);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(params.getDocIdPrefix() + docId);

    BlobItemProperties properties = blobItem.getProperties();
    doc.setField(FileConnector.FILE_PATH, fullPath);

    if (properties.getLastModified() != null) {
      doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    }

    if (properties.getCreationTime() != null) {
      doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    }

    doc.setField(FileConnector.SIZE, properties.getContentLength());

    return doc;
  }

  @Override
  public Document toDoc(TraversalParams params, byte[] contents) {
    Document doc = toDoc(params);

    if (params.shouldGetFileContent()) {
      doc.setField(FileConnector.CONTENT, contents);
    }

    return doc;
  }

  @Override
  public Document toDoc(InputStream in, String decompressedFullPathStr, TraversalParams params) throws IOException {
    String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    Document doc = Document.create(params.getDocIdPrefix() + docId);

    BlobItemProperties properties = blobItem.getProperties();
    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);

    if (properties.getLastModified() != null) {
      doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    }

    if (properties.getCreationTime() != null) {
      doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    }

    // unable to get the decompressed size via inputStream
    if (params.shouldGetFileContent()) {
      doc.setField(FileConnector.CONTENT, in.readAllBytes());
    }

    return doc;
  }

  public BlobItem getBlobItem() {
    return blobItem;
  }
}
