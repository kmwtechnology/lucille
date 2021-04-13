package com.kmwllc.lucille.producer.datatype;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Base64;
import java.util.Objects;

public class FileInfo {
  private String id;
  @JsonProperty("file_path")
  private String filePath;
  // TODO: maybe base64 if it doesn't work
  @JsonProperty("file_content")
  private byte[] fileContent;
  @JsonProperty("file_modification_date")
  private Instant fileModificationDate;
  @JsonProperty("file_creation_date")
  private Instant fileCreationDate;
  @JsonProperty("file_size_bytes")
  private Long fileSizeBytes;
  @JsonIgnore
  private static final Base64.Encoder encoder = Base64.getEncoder();

  private FileInfo() {}

  public FileInfo(Path file, BasicFileAttributes attrs) throws IOException {
    this(file, attrs, false);
  }

  public FileInfo(Path file, BasicFileAttributes attrs, boolean includeFile) throws IOException {
    this.filePath = file.toString();
    // TODO: do we want to use attrs.fileKey() for this?
    final MessageDigest digest;
    try {
      digest = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException("MD5 message digest not available");
    }
    id = encoder.encodeToString(digest.digest(filePath.getBytes(StandardCharsets.UTF_8)));
    fileModificationDate = attrs.lastModifiedTime().toInstant();
    fileCreationDate = attrs.creationTime().toInstant();
    fileSizeBytes = attrs.size();

    if (!includeFile) {
      return;
    }

    fileContent = Files.readAllBytes(file);
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof FileInfo)) {
      return false;
    }

    return hashCode() == other.hashCode();
  }

  @Override
  public int hashCode() {
    return Objects.hash(id, filePath, Arrays.hashCode(fileContent), fileModificationDate, fileCreationDate,
        fileSizeBytes);
  }

  @Override
  public String toString() {
    return "FileInfo{" + "id: " +
        id +
        ", file_path: " +
        filePath +
        ", file_creation_date: " +
        fileCreationDate +
        ", file_modification_date: " +
        fileModificationDate +
        ", file_size_bytes: " +
        fileSizeBytes +
        "}";
  }

  public String getId() {
    return id;
  }

  public String getFilePath() {
    return filePath;
  }

  public byte[] getFileContent() {
    return fileContent;
  }

  public String getFileModificationDate() {
    return fileModificationDate.toString();
  }

  private void setFileModificationDate(String instant) {
    fileModificationDate = Instant.parse(instant);
  }

  public String getFileCreationDate() {
    return fileCreationDate.toString();
  }

  private void setFileCreationDate(String instant) {
    fileCreationDate = Instant.parse(instant);
  }

  public Long getFileSizeBytes() {
    return fileSizeBytes;
  }

  public boolean hasFileContent() {
    return fileContent != null;
  }
}
