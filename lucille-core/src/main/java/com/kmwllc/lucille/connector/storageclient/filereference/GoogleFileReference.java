package com.kmwllc.lucille.connector.storageclient.filereference;

import com.google.cloud.storage.Blob;
import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import java.net.URI;

/**
 * A file reference to a file on Google cloud.
 */
public class GoogleFileReference extends BaseFileReference {

  private final Blob blob;

  public GoogleFileReference(Blob blob) {
    super(blob.getUpdateTimeOffsetDateTime().toInstant());

    this.blob = blob;
  }

  @Override
  public String getFilePath(TraversalParams params) {
    URI paramsURI = params.getURI();
    return paramsURI.getScheme() + "://" + paramsURI.getAuthority() + "/" + blob.getName();
  }

  @Override
  public boolean isCloudFileReference() {
    return true;
  }

  @Override
  public boolean validFile() {
    return !blob.isDirectory();
  }

  public Blob getBlob() {
    return blob;
  }
}
