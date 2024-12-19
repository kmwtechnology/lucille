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
  public void init() {
    AwsBasicCredentials awsCred = AwsBasicCredentials.create((String) cloudOptions.get(FileConnector.S3_ACCESS_KEY_ID),
        (String) cloudOptions.get(FileConnector.S3_SECRET_ACCESS_KEY));
    s3 = S3Client
        .builder()
        .region(Region.of((String) cloudOptions.get(FileConnector.S3_REGION)))
        .credentialsProvider(StaticCredentialsProvider.create(awsCred))
        .build();

    try {
      initializeFileHandlers();
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Error occurred initializing FileHandlers", e);
    }
  }

  @Override
  public void shutdown() throws Exception {
    if (s3 != null) {
      s3.close();
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
              String pathStr = obj.key();
              String fileExtension = FilenameUtils.getExtension(pathStr);
              tryProcessAndPublishFile(publisher, pathStr, fileExtension, new FileReference(obj));
            }
          });
        });
  }

  @Override
  public Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName) {
    S3Object obj = fileReference.getS3Object();
    return s3ObjectToDoc(obj, bucketOrContainerName);
  }

  @Override
  public Document convertFileReferenceToDoc(FileReference fileReference, String bucketOrContainerName, InputStream in, String fileName) {
    S3Object obj = fileReference.getS3Object();
    try {
      return s3ObjectToDoc(obj, bucketOrContainerName, in, fileName);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert S3Object '" + obj.key() + "' to Document", e);
    }
  }

  @Override
  public byte[] getFileReferenceContent(FileReference fileReference) {
    S3Object obj = fileReference.getS3Object();
    return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketOrContainerName).key(obj.key()).build()).asByteArray();
  }

  @Override
  public InputStream getFileReferenceContentStream(FileReference fileReference) {
    byte[] content = getFileReferenceContent(fileReference);
    return new ByteArrayInputStream(content);
  }

  private Document s3ObjectToDoc(S3Object obj, String bucketName) {
    String objKey = obj.key();
    String docId = DigestUtils.md5Hex(objKey);
    Document doc = Document.create(docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + objKey);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    // s3 doesn't have object creation date
    doc.setField(FileConnector.SIZE, obj.size());

    if (getFileContent) {
      byte[] content = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(objKey).build()).asByteArray();
      doc.setField(FileConnector.CONTENT, content);
    }

    return doc;
  }

  private Document s3ObjectToDoc(S3Object obj, String bucketName, InputStream is, String fileNameWithoutExtension)
      throws IOException {
    String docId = DigestUtils.md5Hex(fileNameWithoutExtension);
    Document doc = Document.create(docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + fileNameWithoutExtension);
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

  // Only for testing
  void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }
}
