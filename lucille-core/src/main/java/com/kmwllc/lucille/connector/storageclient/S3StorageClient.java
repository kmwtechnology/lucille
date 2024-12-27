package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

public class S3StorageClient extends BaseStorageClient {

  private S3Client s3;
  private static final Logger log = LoggerFactory.getLogger(S3StorageClient.class);

  public S3StorageClient(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public void init() throws ConnectorException {
    try {
      AwsBasicCredentials awsCred = AwsBasicCredentials.create((String) cloudOptions.get(FileConnector.S3_ACCESS_KEY_ID),
          (String) cloudOptions.get(FileConnector.S3_SECRET_ACCESS_KEY));
      s3 = S3Client
          .builder()
          .region(Region.of((String) cloudOptions.get(FileConnector.S3_REGION)))
          .credentialsProvider(StaticCredentialsProvider.create(awsCred))
          .build();
    } catch (Exception e) {
      throw new ConnectorException("Error occurred building S3Client", e);
    }

    initializeFileHandlers();
  }

  @Override
  public void shutdown() throws IOException {
    if (s3 != null) {
      try {
        s3.close();
      } catch (Exception e) {
        throw new IOException("Error occurred closing S3Client", e);
      }
    }

    // clear all FileHandlers if any
    clearFileHandlers();
  }

  @Override
  public void traverse(Publisher publisher) throws Exception {
    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketOrContainerName).prefix(startingDirectory).maxKeys(maxNumOfPages).build();
    ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);
    response.stream()
        .forEach(resp -> {
          resp.contents().forEach(obj -> {
            if (isValid(obj)) {
              String fullPathStr = getFullPath(obj);
              String fileExtension = FilenameUtils.getExtension(fullPathStr);
              tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(obj));
            }
          });
        });
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference) {
    S3Object obj = fileReference.getS3Object();
    return s3ObjectToDoc(obj);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr) {
    S3Object obj = fileReference.getS3Object();
    try {
      return s3ObjectToDoc(obj, in, decompressedFullPathStr);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert S3Object '" + obj.key() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference) {
    S3Object obj = fileReference.getS3Object();
    return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketOrContainerName).key(obj.key()).build()).asByteArray();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference) {
    byte[] content = getFileReferenceContent(fileReference);
    return new ByteArrayInputStream(content);
  }

  private Document s3ObjectToDoc(S3Object obj) {
    String fullPath = getFullPath(obj);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, fullPath);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    // s3 doesn't have object creation date
    doc.setField(FileConnector.SIZE, obj.size());

    if (getFileContent) {
      byte[] content = s3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(bucketOrContainerName).key(obj.key()).build()
      ).asByteArray();
      doc.setField(FileConnector.CONTENT, content);
    }

    return doc;
  }

  private Document s3ObjectToDoc(S3Object obj, InputStream is, String decompressedFullPathStr)
      throws IOException {
    String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    Document doc = Document.create(docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    // s3 doesn't have object creation date
    // compression stream doesn't have size, so we can't set it here
    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, is.readAllBytes());
    }

    return doc;
  }

  private boolean isValid(S3Object obj) {
    String key = obj.key();
    if (key.endsWith("/")) return false;

    return shouldIncludeFile(key, includes, excludes);
  }

  private String getFullPath(S3Object obj) {
    return pathToStorageURI.getScheme() + "://" + bucketOrContainerName + "/" + obj.key();
  }

  // Only for testing
  void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }
}
