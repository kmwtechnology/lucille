package com.kmwllc.lucille.util;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {
  @Test
  public void testIsValidURI() {
    String validURI = "file:///path/to/file";
    assertTrue(FileUtils.isValidURI(validURI));

    String nullScheme = "example.org";
    assertFalse(FileUtils.isValidURI(nullScheme));

    String exceptionURI = ":example.org";
    assertFalse(FileUtils.isValidURI(exceptionURI));
  }
}
