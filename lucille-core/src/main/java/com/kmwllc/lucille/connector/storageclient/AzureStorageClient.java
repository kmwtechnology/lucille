package com.kmwllc.lucille.connector.storageclient;

import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobItemProperties;
import com.azure.storage.blob.models.ListBlobsOptions;
import com.azure.storage.common.StorageSharedKeyCredential;
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
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.compress.compressors.CompressorInputStream;
import org.apache.commons.compress.compressors.CompressorStreamFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

public class AzureStorageClient extends BaseStorageClient {

  private BlobContainerClient containerClient;
  private static final Logger log = LoggerFactory.getLogger(AzureStorageClient.class);

  public AzureStorageClient(URI pathToStorage, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions, Config fileOptions) {
    super(pathToStorage, docIdPrefix, excludes, includes, cloudOptions, fileOptions);
  }

  @Override
  public String getContainerOrBucketName() {
    return pathToStorageURI.getPath().split("/")[1];
  }

  @Override
  public String getStartingDirectory() {
    String path = pathToStorageURI.getPath();
    String[] subPaths = path.split("/", 3);
    return subPaths.length > 2 ? subPaths[2] : "";
  }

  @Override
  public void init() {
    if (cloudOptions.containsKey(FileConnector.AZURE_CONNECTION_STRING)) {
      containerClient = new BlobContainerClientBuilder()
          .connectionString((String) cloudOptions.get(FileConnector.AZURE_CONNECTION_STRING))
          .containerName(bucketOrContainerName)
          .buildClient();
    } else {
      String accountName = (String) cloudOptions.get(FileConnector.AZURE_ACCOUNT_NAME);
      String accountKey = (String) cloudOptions.get(FileConnector.AZURE_ACCOUNT_KEY);

      containerClient = new BlobContainerClientBuilder()
          .credential(new StorageSharedKeyCredential(accountName, accountKey))
          .containerName(bucketOrContainerName)
          .buildClient();
    }

    try {
      initializeFileHandlers();
    } catch (ConnectorException e) {
      throw new IllegalArgumentException("Error occurred initializing FileHandlers", e);
    }
  }

  @Override
  public void shutdown() throws Exception {
    // azure container client is not closable
    containerClient = null;
    // clear all file handlers if any
    clearFileHandlers();
  }

  @Override
  public void traverse(Publisher publisher) throws Exception{
    containerClient.listBlobs(new ListBlobsOptions().setPrefix(startingDirectory).setMaxResultsPerPage(maxNumOfPages), Duration.ofSeconds(10)).stream()
        .forEach(blob -> {
          if (isValid(blob)) {
            try {
              String pathStr = blob.getName();
              String fileExtension = FilenameUtils.getExtension(pathStr);

              // handle compressed files if needed
              if (handleCompressedFiles && isSupportedCompressedFileType(pathStr)) {
                byte[] content = containerClient.getBlobClient(blob.getName()).downloadContent().toBytes();
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
                    Document doc = blobItemToDoc(blob, bucketOrContainerName, compressorStream, unzippedFileName);
                    publisher.publish(doc);
                  }
                }
                return;
              }

              // handle archived files if needed
              if (handleArchivedFiles && isSupportedArchiveFileType(pathStr)) {
                byte[] content = containerClient.getBlobClient(blob.getName()).downloadContent().toBytes();
                try (InputStream is = new ByteArrayInputStream(content)) {
                  handleArchiveFiles(publisher, is);
                }
                return;
              }

              // handle file types if needed
              if (!fileOptions.isEmpty() && FileHandler.supportAndContainFileType(fileExtension, fileOptions)) {
                // get the file content
                byte[] content = containerClient.getBlobClient(blob.getName()).downloadContent().toBytes();
                // instantiate the right FileHandler and publish based on content
                publishUsingFileHandler(publisher, fileExtension, content, pathStr);
                return;
              }

              Document doc = blobItemToDoc(blob, bucketOrContainerName);
              publisher.publish(doc);
            } catch (Exception e) {
              log.error("Error publishing blob: {}", blob.getName(), e);
            }
          }
        });
  }

  private Document blobItemToDoc(BlobItem blob, String container) {
    String docId = DigestUtils.md5Hex(blob.getName());
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    String path = String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        container, blob.getName());
    doc.setField(FileConnector.FILE_PATH, path);
    doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    doc.setField(FileConnector.SIZE, properties.getContentLength());

    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, containerClient.getBlobClient(blob.getName()).downloadContent().toBytes());
    }

    return doc;
  }

  private Document blobItemToDoc(BlobItem blob, String container, InputStream is, String unzippedFileName)
      throws IOException {
    String docId = DigestUtils.md5Hex(unzippedFileName);
    Document doc = Document.create(docIdPrefix + docId);
    BlobItemProperties properties = blob.getProperties();
    String path = String.format("%s://%s/%s/%s", pathToStorageURI.getScheme(), pathToStorageURI.getAuthority(),
        container, unzippedFileName);
    doc.setField(FileConnector.FILE_PATH, path);
    doc.setField(FileConnector.MODIFIED, properties.getLastModified().toInstant());
    doc.setField(FileConnector.CREATED, properties.getCreationTime().toInstant());
    // unable to get the decompressed size
    if (getFileContent) {
      doc.setField(FileConnector.CONTENT, is.readAllBytes());
    }

    return doc;
  }

  private boolean isValid(BlobItem blob) {
    if (blob.isPrefix()) return false;

    return shouldIncludeFile(blob.getName(), includes, excludes);
  }

  // Only for testing
  void setContainerClientForTesting(BlobContainerClient containerClient) {
    this.containerClient = containerClient;
  }
}
