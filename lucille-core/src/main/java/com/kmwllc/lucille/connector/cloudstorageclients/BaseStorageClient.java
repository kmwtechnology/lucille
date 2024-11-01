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
  protected String bucketName;
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
    this.bucketName = pathToStorageURI.getAuthority();
    this.startingDirectory = Objects.equals(pathToStorageURI.getPath(), "/") ? "" : pathToStorageURI.getPath();
    if (this.startingDirectory.startsWith("/")) this.startingDirectory = this.startingDirectory.substring(1);
    this.excludes = excludes;
    this.includes = includes;
    this.cloudOptions = cloudOptions;
    this.maxNumOfPages = cloudOptions.containsKey("maxNumOfPages") ? (Integer) cloudOptions.get("maxNumOfPages") : 100;
  }

  public boolean shouldSkipBasedOnRegex(String fileName) {
      return excludes.stream().anyMatch(pattern -> pattern.matcher(fileName).matches())
          || (!includes.isEmpty() && includes.stream().noneMatch(pattern -> pattern.matcher(fileName).matches()));
  }
}
