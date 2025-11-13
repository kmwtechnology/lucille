package com.kmwllc.lucille.connector.storageclient;

import static com.kmwllc.lucille.connector.FileConnector.S3_ACCESS_KEY_ID;
import static com.kmwllc.lucille.connector.FileConnector.S3_REGION;
import static com.kmwllc.lucille.connector.FileConnector.S3_SECRET_ACCESS_KEY;

import com.kmwllc.lucille.connector.FileConnectorStateManager;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.paginators.ListObjectsV2Iterable;

/**
 * A storage client for S3. Create using a configuration (commonly mapped to <b>s3</b>) which can contain
 * "region" and can contain <b>both</b> "accessKeyId" and "secretAccessKey".
 */
public class S3StorageClient extends BaseStorageClient {

  private S3Client s3;
  private static final Logger log = LoggerFactory.getLogger(S3StorageClient.class);

  public S3StorageClient(Config s3CloudOptions) {
    super(s3CloudOptions);
  }

  @Override
  protected void validateOptions(Config config) {
    if (config.hasPath(S3_ACCESS_KEY_ID) ^ config.hasPath(S3_SECRET_ACCESS_KEY)) {
      throw new IllegalArgumentException("'" + S3_ACCESS_KEY_ID + "' and '" + S3_SECRET_ACCESS_KEY +
          "' must be specified together or omitted together in Config for S3StorageClient.");
    }
  }

  @Override
  protected void initializeStorageClient() throws IOException {
    try {
      S3ClientBuilder builder = S3Client.builder();

      if (config.hasPath(S3_REGION)) {
        Region configRegion = Region.of(config.getString(S3_REGION));
        builder = builder.region(configRegion);
      }

      // use StaticCredentialsProvider when access key is provided,
      // otherwise don't set a credentials provider but instead implicitly use the default credentials provider chain
      if (config.hasPath(S3_ACCESS_KEY_ID) && config.hasPath(S3_SECRET_ACCESS_KEY)) {
        AwsBasicCredentials awsCred = AwsBasicCredentials.create(config.getString(S3_ACCESS_KEY_ID), config.getString(S3_SECRET_ACCESS_KEY));
        builder = builder.credentialsProvider(StaticCredentialsProvider.create(awsCred));
      }

      s3 = builder.build();
    } catch (Exception e) {
      throw new IOException("Error occurred building S3Client", e);
    }
  }

  @Override
  protected void shutdownStorageClient() throws IOException {
    if (s3 != null) {
      try {
        s3.close();
      } catch (Exception e) {
        throw new IOException("Error occurred closing S3Client", e);
      }
    }
  }

  @Override
  protected void traverseStorageClient(Publisher publisher, TraversalParams params, FileConnectorStateManager stateMgr) throws Exception {
    ListObjectsV2Request request = ListObjectsV2Request.builder()
        .bucket(getBucketOrContainerName(params))
        .prefix(getStartingDirectory(params))
        .maxKeys(maxNumOfPages).build();
    ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);
    response.stream()
        .forEachOrdered(resp -> {
          resp.contents().forEach(obj -> {
            S3FileReference fileRef = new S3FileReference(obj, params);
            processAndPublishFileIfValid(publisher, fileRef, params, stateMgr);
          });
        });
  }

  @Override
  protected InputStream getFileContentStreamFromStorage(URI uri) throws IOException {
    String bucketName = uri.getAuthority();
    String objectKey = uri.getPath().substring(1);

    GetObjectRequest request = GetObjectRequest.builder().bucket(bucketName).key(objectKey).build();
    return s3.getObject(request);
  }

  @Override
  public void moveFile(URI filePath, URI folder) throws IOException {
    String sourceBucket = filePath.getAuthority();
    String sourceKey = filePath.getPath().substring(1);

    String destBucket = folder.getAuthority();
    String destKey = folder.getPath().substring(1) + sourceKey;

    CopyObjectRequest copyRequest = CopyObjectRequest.builder()
        .sourceBucket(sourceBucket).sourceKey(sourceKey)
        .destinationBucket(destBucket).destinationKey(destKey)
        .build();

    DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
        .bucket(sourceBucket).key(sourceKey)
        .build();

    s3.copyObject(copyRequest);
    s3.deleteObject(deleteRequest);
  }

  private String getStartingDirectory(TraversalParams params) {
    URI pathURI = params.getURI();
    String startingDirectory = Objects.equals(pathURI.getPath(), "/") ? "" : pathURI.getPath();
    if (startingDirectory.startsWith("/")) {
      return startingDirectory.substring(1);
    }
    return startingDirectory;
  }

  private String getBucketOrContainerName(TraversalParams params) {
    return params.getURI().getAuthority();
  }

  // Only for testing
  void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }


  private class S3FileReference extends BaseFileReference {

    private final S3Object s3Obj;

    public S3FileReference(S3Object s3Obj, TraversalParams params) {
      // These are inexpensive calls - information stored in the s3 object.
      // "null" for creation time - this isn't available in a S3Object
      super(getFullPathHelper(s3Obj, params), s3Obj.lastModified(), s3Obj.size(), null);

      this.s3Obj = s3Obj;
    }

    @Override
    public String getName() {
      return s3Obj.key();
    }

    @Override
    public boolean isValidFile() {
      return !s3Obj.key().endsWith("/");
    }

    @Override
    public InputStream getContentStream(TraversalParams params) {
      String objKey = s3Obj.key();
      GetObjectRequest objectRequest = GetObjectRequest.builder().bucket(getBucketOrContainerName(params)).key(objKey).build();
      return s3.getObject(objectRequest);
    }

    @Override
    protected byte[] getFileContent(TraversalParams params) {
      return s3.getObjectAsBytes(
          GetObjectRequest.builder().bucket(getBucketOrContainerName(params)).key(s3Obj.key()).build()
      ).asByteArray();
    }

    private static URI getFullPathHelper(S3Object s3Obj, TraversalParams params) {
      URI paramsURI = params.getURI();

      try {
        return new URI(paramsURI.getScheme(), paramsURI.getAuthority(), "/" + s3Obj.key(), null);
      } catch (Exception e) {
        throw new IllegalArgumentException("Unable to build S3 URI for key: " + s3Obj.key(), e);
      }
    }
  }
}
