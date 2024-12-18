package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
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
              try {
                beforeProcessingFile(pathStr);

                // handle compressed files if needed
                if (handleCompressedFiles && isSupportedCompressedFileType(pathStr)) {
                  byte[] content = s3.getObjectAsBytes(
                      GetObjectRequest.builder().bucket(bucketOrContainerName).key(pathStr).build()
                  ).asByteArray();
                  // unzip the file, compressorStream will be closed when try block is exited
                  try (BufferedInputStream bis = new BufferedInputStream(new ByteArrayInputStream(content));
                      CompressorInputStream compressorStream = new CompressorStreamFactory().createCompressorInputStream(bis)) {
                    // we can remove the last extension from path knowing before we confirmed that it has a compressed extension
                    String unzippedFileName = FilenameUtils.removeExtension(pathStr);
                    if (handleArchivedFiles && isSupportedArchiveFileType(unzippedFileName)) {
                      handleArchiveFiles(publisher, compressorStream);
                    } else if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(FilenameUtils.getExtension(unzippedFileName), fileOptions)) {
                      handleStreamExtensionFiles(publisher, compressorStream, FilenameUtils.getExtension(unzippedFileName), pathStr);
                    } else {
                      Document doc = s3ObjectToDoc(obj, bucketOrContainerName, compressorStream, unzippedFileName);
                      publisher.publish(doc);
                    }
                  }
                  afterProcessingFile(pathStr);
                  return;
                }

                // handle archive files if needed
                if (handleArchivedFiles && isSupportedArchiveFileType(pathStr)) {
                  byte[] content = s3.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucketOrContainerName).key(pathStr).build()
                  ).asByteArray();

                  try (InputStream is = new ByteArrayInputStream(content)) {
                    handleArchiveFiles(publisher, is);
                  }
                  afterProcessingFile(pathStr);
                  return;
                }

                // handle file types if needed
                if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions)) {
                  // get the file content
                  byte[] content = s3.getObjectAsBytes(
                    GetObjectRequest.builder().bucket(bucketOrContainerName).key(pathStr).build()
                  ).asByteArray();

                  // instantiate the right FileHandler and publish based on content
                  publishUsingFileHandler(publisher, fileExtension, content, pathStr);
                  afterProcessingFile(pathStr);
                  return;
                }

                Document doc = s3ObjectToDoc(obj, bucketOrContainerName);
                publisher.publish(doc);
                afterProcessingFile(pathStr);
              } catch (Exception e) {
                try {
                  errorProcessingFile(pathStr);
                } catch (IOException ex) {
                  log.error("Error occurred while performing error operations on file '{}'", pathStr, ex);
                  throw new RuntimeException(ex);
                }
                log.error("Unable to publish document '{}', SKIPPING", obj.key(), e);
              }
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
