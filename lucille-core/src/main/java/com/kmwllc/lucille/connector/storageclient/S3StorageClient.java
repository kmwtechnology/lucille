package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Map;
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

  // Constructor for a client used to extract InputStreams from individual URIs.
  public S3StorageClient(Map<String, Object> cloudOptions) {
    super(cloudOptions);
  }

  @Override
  public void init() throws ConnectorException {
    try {
      AwsBasicCredentials awsCred = AwsBasicCredentials.create((String) cloudOptions.get(S3_ACCESS_KEY_ID),
          (String) cloudOptions.get(S3_SECRET_ACCESS_KEY));
      s3 = S3Client
          .builder()
          .region(Region.of((String) cloudOptions.get(S3_REGION)))
          .credentialsProvider(StaticCredentialsProvider.create(awsCred))
          .build();
    } catch (Exception e) {
      throw new ConnectorException("Error occurred building S3Client", e);
    }
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
  }

  @Override
  public void traverse(Publisher publisher, TraversalParams params) throws Exception {
    try {
      initializeFileHandlers(params);

      ListObjectsV2Request request = ListObjectsV2Request.builder()
          .bucket(params.bucketOrContainerName)
          .prefix(params.startingDirectory)
          .maxKeys(maxNumOfPages).build();
      ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);
      response.stream()
          .forEachOrdered(resp -> {
            resp.contents().forEach(obj -> {
              if (isValid(obj, params)) {
                String fullPathStr = getFullPath(obj, params);
                String fileExtension = FilenameUtils.getExtension(fullPathStr);
                tryProcessAndPublishFile(publisher, fullPathStr, fileExtension, new FileReference(obj), params);
              }
            });
          });
    } finally {
      clearFileHandlers();
    }
  }

  @Override
  public InputStream getFileContentStream(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();
    return s3.getObject(request);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, TraversalParams params) {
    S3Object obj = fileReference.getS3Object();
    return s3ObjectToDoc(obj, params);
  }

  @Override
  protected Document convertFileReferenceToDoc(FileReference fileReference, InputStream in, String decompressedFullPathStr, TraversalParams params) {
    S3Object obj = fileReference.getS3Object();
    try {
      return s3ObjectToDoc(obj, in, decompressedFullPathStr, params);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to convert S3Object '" + obj.key() + "' to Document", e);
    }
  }

  @Override
  protected byte[] getFileReferenceContent(FileReference fileReference, TraversalParams params) {
    S3Object obj = fileReference.getS3Object();
    return s3.getObjectAsBytes(GetObjectRequest.builder().bucket(params.bucketOrContainerName).key(obj.key()).build()).asByteArray();
  }

  @Override
  protected InputStream getFileReferenceContentStream(FileReference fileReference, TraversalParams params) {
    byte[] content = getFileReferenceContent(fileReference, params);
    return new ByteArrayInputStream(content);
  }

  private Document s3ObjectToDoc(S3Object obj, TraversalParams params) {
    String fullPath = getFullPath(obj, params);
    String docId = DigestUtils.md5Hex(fullPath);
    Document doc = Document.create(params.docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, fullPath);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    // s3 doesn't have object creation date
    doc.setField(FileConnector.SIZE, obj.size());

    if (params.getFileContent) {
      byte[] content = s3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(params.bucketOrContainerName).key(obj.key()).build()
      ).asByteArray();
      doc.setField(FileConnector.CONTENT, content);
    }

    return doc;
  }

  private Document s3ObjectToDoc(S3Object obj, InputStream is, String decompressedFullPathStr, TraversalParams params)
      throws IOException {
    String docId = DigestUtils.md5Hex(decompressedFullPathStr);
    Document doc = Document.create(params.docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, decompressedFullPathStr);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    // s3 doesn't have object creation date
    // compression stream doesn't have size, so we can't set it here
    if (params.getFileContent) {
      doc.setField(FileConnector.CONTENT, is.readAllBytes());
    }

    return doc;
  }

  private boolean isValid(S3Object obj, TraversalParams params) {
    String key = obj.key();
    if (key.endsWith("/")) return false;

    return params.shouldIncludeFile(key);
  }

  private String getFullPath(S3Object obj, TraversalParams params) {
    return params.pathToStorageURI.getScheme() + "://" + params.bucketOrContainerName + "/" + obj.key();
  }

  public static void validateOptions(Map<String, Object> cloudOptions) {
    if (!validOptions(cloudOptions)) {
      throw new IllegalArgumentException("Missing '" + S3_ACCESS_KEY_ID + "' or '" + S3_SECRET_ACCESS_KEY + "' or '" + S3_REGION + "' in cloudOptions for S3StorageClient.");
    }
  }

  public static boolean validOptions(Map<String, Object> cloudOptions) {
    return cloudOptions.containsKey(S3_ACCESS_KEY_ID)
        && cloudOptions.containsKey(S3_SECRET_ACCESS_KEY)
        && cloudOptions.containsKey(S3_REGION);
  }

  // Only for testing
  void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }
}
