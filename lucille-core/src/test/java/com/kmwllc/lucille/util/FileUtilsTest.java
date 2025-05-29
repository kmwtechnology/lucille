package com.kmwllc.lucille.util;

import java.nio.file.Paths;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {

  @Test
  public void testGetLucilleHomeDirectory() {
    String oldLucilleHome = System.getProperty("LUCILLE_HOME");
    String oldUserDir = System.getProperty("user.dir");

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

    if (oldLucilleHome != null) {
      System.setProperty("LUCILLE_HOME", oldLucilleHome);
    }

    if (oldUserDir != null) {
      System.setProperty("user.dir", oldUserDir);
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
}
