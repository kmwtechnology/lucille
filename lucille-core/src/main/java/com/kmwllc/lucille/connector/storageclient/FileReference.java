package com.kmwllc.lucille.connector.storageclient;

import com.azure.storage.blob.models.BlobItem;
import com.google.cloud.storage.Blob;
import java.nio.file.Path;
import java.time.Instant;
import software.amazon.awssdk.services.s3.model.S3Object;

/**
 * Stores a reference to a file in a cloud storage service or a local file system.
 */

public class FileReference {
  Path path;
  S3Object s3Object;
  BlobItem blobItem;
  Blob blob;
  final Instant lastModified;

  public FileReference(Path path, Instant lastModifiedInstant) {
    this.path = path;
    this.lastModified = lastModifiedInstant;
  }

  public FileReference(S3Object s3Object) {
    this.s3Object = s3Object;
    this.lastModified = s3Object.lastModified();
  }

  public FileReference(BlobItem blobItem) {
    this.blobItem = blobItem;
    this.lastModified = blobItem.getProperties().getLastModified().toInstant();
  }

  public FileReference(Blob blob) {
    this.blob = blob;
    this.lastModified = blob.getUpdateTimeOffsetDateTime().toInstant();
  }

  public Path getPath() {
    return path;
  }

  public S3Object getS3Object() {
    return s3Object;
  }

  public BlobItem getBlobItem() {
    return blobItem;
  }

  public Blob getBlob() {
    return blob;
  }

  public Instant getLastModified() { return lastModified; }

  public boolean isCloudFileReference() {
    return s3Object != null || blobItem != null || blob != null;
  }
}
