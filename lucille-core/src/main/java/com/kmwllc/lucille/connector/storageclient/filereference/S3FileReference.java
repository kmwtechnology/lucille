package com.kmwllc.lucille.connector.storageclient.filereference;

import com.kmwllc.lucille.connector.storageclient.TraversalParams;
import java.net.URI;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * A file reference to a file on S3.
 */
public class S3FileReference extends BaseFileReference {

  private final S3Object s3Obj;

  public S3FileReference(S3Object s3Obj) {
    super(s3Obj.lastModified());

    this.s3Obj = s3Obj;
  }

  @Override
  public String getFilePath(TraversalParams params) {
    URI paramsURI = params.getURI();
    return paramsURI.getScheme() + "://" + paramsURI.getAuthority() + "/" + s3Obj.key();
  }

  @Override
  public boolean isCloudFileReference() {
    return true;
  }

  @Override
  public boolean validFile() {
    return !s3Obj.key().endsWith("/");
  }

  public S3Object getS3Obj() {
    return s3Obj;
  }
}
