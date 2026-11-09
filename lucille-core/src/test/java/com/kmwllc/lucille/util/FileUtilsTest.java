package com.kmwllc.lucille.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {

  @Test
  public void testGetLucilleHomeDirectory() {
    String oldLucilleHome = System.getProperty("LUCILLE_HOME");
    String oldUserDir = System.getProperty("user.dir");

    try {
      // 1. Getting it when it is set to a String
      // This runs Path.of(""), which returns the working directory (lucille-core).
      System.setProperty("LUCILLE_HOME", "");
      String lucilleHome = FileUtils.getLucilleHomeDirectory();
      assertTrue(Paths.get(lucilleHome).isAbsolute());
      assertTrue(Paths.get(lucilleHome).endsWith("lucille-core"));

      // 2. Getting it when it is not set, falls back to user.dir
      System.clearProperty("LUCILLE_HOME");
      System.setProperty("user.dir", "/test/folder");
      String homeDir = FileUtils.getLucilleHomeDirectory();
      assertEquals("/test/folder", homeDir);
    } finally {
      if (oldLucilleHome != null) {
        System.setProperty("LUCILLE_HOME", oldLucilleHome);
      } else {
        System.clearProperty("LUCILLE_HOME");
      }

      if (oldUserDir != null) {
        System.setProperty("user.dir", oldUserDir);
      } else {
        System.clearProperty("user.dir");
      }
    }
  }

  @Test
  public void testIsValidURI() {
    String validURI = "file:///path/to/file";
    assertTrue(FileUtils.isValidURI(validURI));

    String nullScheme = "example.org";
    assertFalse(FileUtils.isValidURI(nullScheme));

    String exceptionURI = ":example.org";
    assertFalse(FileUtils.isValidURI(exceptionURI));
  }
  
  @Test
  public void testAppendStringToFileName() {
    assertEquals("/Users/Desktop/hello-FILE-1.txt", FileUtils.appendStringToFileName("/Users/Desktop/hello.txt", "-FILE-1"));
    assertEquals("/Users/Desktop/hello-FILE-1", FileUtils.appendStringToFileName("/Users/Desktop/hello", "-FILE-1"));
  }

  @Test
  public void testReadAllBytes() throws IOException {
    byte[] content = new byte[100];

    assertEquals(100, FileUtils.readAllBytes(new ByteArrayInputStream(content), null).length);
    assertEquals(100, FileUtils.readAllBytes(new ByteArrayInputStream(content), 100L).length);
    assertEquals(100, FileUtils.readAllBytes(new ByteArrayInputStream(content), 1000L).length);
    assertThrows(FileUtils.ContentTooLargeException.class, () -> FileUtils.readAllBytes(new ByteArrayInputStream(content), 99L));
  }

  // A Windows drive path is a local file, not a URI with a one-letter scheme. Holds on any OS.
  @Test
  public void testStorageURIFor() {
    for (String drivePath : java.util.List.of("C:/data/report.csv", "C:\\data\\report.csv")) {
      java.net.URI uri = FileUtils.storageURIFor(drivePath);
      assertEquals("file", uri.getScheme());
      assertTrue(uri.getPath().endsWith("report.csv"));
    }

    assertEquals("s3", FileUtils.storageURIFor("s3://bucket/key.csv").getScheme());
    assertEquals("file", FileUtils.storageURIFor("file:///tmp/a.csv").getScheme());

    // plain local paths are opened directly
    assertNull(FileUtils.storageURIFor("relative/a.csv"));
    assertNull(FileUtils.storageURIFor("/absolute/a.csv"));
  }

  @Test
  public void testParsePathOrURIKeepsValidURIs() throws Exception {
    assertEquals(URI.create("s3://bucket/key"), FileUtils.parsePathOrURI("s3://bucket/key"));
    assertEquals(URI.create("temp/defaults.csv"), FileUtils.parsePathOrURI("temp/defaults.csv"));
  }

  @Test
  public void testParsePathOrURITreatsDriveLetterAsLocalPath() throws Exception {
    // a single-letter "scheme" is a Windows drive letter, not a URI scheme
    assertEquals("file", FileUtils.parsePathOrURI("C:/data").getScheme());
    assertEquals("file", FileUtils.parsePathOrURI("C:\\data").getScheme());
  }

  @Test
  public void testParsePathOrURIConvertsInvalidURIToLocalPath() throws Exception {
    assertEquals(Paths.get("folder with spaces/file.txt").toUri(), FileUtils.parsePathOrURI("folder with spaces/file.txt"));
  }

  @Test
  public void testParsePathOrURIRejectsInvalidURIWithScheme() {
    assertThrows(URISyntaxException.class, () -> FileUtils.parsePathOrURI("s3://bucket/with space"));
  }
}
