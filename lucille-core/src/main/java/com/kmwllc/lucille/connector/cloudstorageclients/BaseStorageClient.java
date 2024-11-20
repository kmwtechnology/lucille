package com.kmwllc.lucille.connector.cloudstorageclients;

import com.kmwllc.lucille.core.Publisher;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

public abstract class BaseStorageClient implements CloudStorageClient {

  protected Publisher publisher;
  protected String docIdPrefix;
  protected URI pathToStorageURI;
  protected String bucketOrContainerName;
  protected String startingDirectory;
  List<Pattern> excludes;
  List<Pattern> includes;
  Map<String, Object> cloudOptions;
  public Integer maxNumOfPages;

  public BaseStorageClient(URI pathToStorageURI, Publisher publisher, String docIdPrefix, List<Pattern> excludes, List<Pattern> includes,
      Map<String, Object> cloudOptions) {
    this.publisher = publisher;
    this.docIdPrefix = docIdPrefix;
    this.pathToStorageURI = pathToStorageURI;
    this.bucketOrContainerName = getContainerOrBucketName();
    this.startingDirectory = getStartingDirectory();
    this.excludes = excludes;
    this.includes = includes;
    this.cloudOptions = cloudOptions;
    this.maxNumOfPages = cloudOptions.containsKey("maxNumOfPages") ? (Integer) cloudOptions.get("maxNumOfPages") : 100;
  }

  public String getContainerOrBucketName() {
    return pathToStorageURI.getAuthority();
  }

  public String getStartingDirectory() {
    String startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (startingDirectory.startsWith("/")) return startingDirectory.substring(1);
    return startingDirectory;
  }
}
