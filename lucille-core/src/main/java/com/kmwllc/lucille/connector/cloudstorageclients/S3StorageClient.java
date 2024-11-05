package com.kmwllc.lucille.connector.cloudstorageclients;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
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
  private static final Logger log = LoggerFactory.getLogger(FileConnector.class);

  public S3StorageClient(URI pathToStorage, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions) {
    super(pathToStorage, publisher, docIdPrefix, excludes, includes, cloudOptions);
  }

  @Override
  public void init() {
    AwsBasicCredentials awsCred = AwsBasicCredentials.create((String) cloudOptions.get("accessKeyId"), (String) cloudOptions.get("secretAccessKey"));
    s3 = S3Client
        .builder()
        .region(Region.of((String) cloudOptions.get("region")))
        .credentialsProvider(StaticCredentialsProvider.create(awsCred))
        .build();
  }

  @Override
  public void shutdown() throws Exception {
    if (s3 != null) {
      s3.close();
    }
  }

  @Override
  public void publishFiles() {
    ListObjectsV2Request request = ListObjectsV2Request.builder().bucket(bucketOrContainerName).prefix(startingDirectory).maxKeys(maxNumOfPages).build();
    ListObjectsV2Iterable response = s3.listObjectsV2Paginator(request);
    response.stream()
        .forEach(resp -> {
          resp.contents().forEach(obj -> {
            if (isNotValid(obj)) {
              return;
            }
            try {
              Document doc = s3ObjectToDoc(obj, bucketOrContainerName);
              publisher.publish(doc);
            } catch (Exception e) {
              log.error("Unable to publish document '{}', SKIPPING", obj.key(), e);
            }
          });
        });
  }

  private Document s3ObjectToDoc(S3Object obj, String bucketName) {
    String objKey = obj.key();
    String docId = DigestUtils.md5Hex(objKey);
    Document doc = Document.create(docIdPrefix + docId);
    doc.setField(FileConnector.FILE_PATH, pathToStorageURI.getScheme() + "://" + bucketName + "/" + objKey);
    doc.setField(FileConnector.MODIFIED, obj.lastModified());
    doc.setField(FileConnector.CREATED, obj.lastModified()); // there isn't an object creation date in S3
    doc.setField(FileConnector.SIZE, obj.size());
    byte[] content = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(bucketName).key(objKey).build()).asByteArray();
    doc.setField(FileConnector.CONTENT, content);

    return doc;
  }

  private boolean isNotValid(S3Object obj) {
    String key = obj.key();
    if (key.endsWith("/")) return true;

    return shouldSkipBasedOnRegex(key);
  }

  // Only for testing
  public void setS3ClientForTesting(S3Client s3) {
    this.s3 = s3;
  }
}
