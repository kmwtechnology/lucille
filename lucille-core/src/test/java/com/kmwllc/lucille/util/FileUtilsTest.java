package com.kmwllc.lucille.util;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.Reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class FileUtilsTest {

  @Test
  public void getLocalReaderTest() throws Exception {
    String path = "classpath:FileUtilsTest/test";
    try (Reader r = FileContentFetcher.getSingleReader(path, Map.of())) {
      assertEquals("aaaaa", IOUtils.toString(r));
    }

    path = "src/test/resources/FileUtilsTest/test";
    try (Reader r = FileContentFetcher.getSingleReader(path, Map.of())) {
      assertEquals("aaaaa", IOUtils.toString(r));
    }

    String badPath = Paths.get("src/test/resources/FileUtilsTest/test").toUri().toString();
    assertThrows(IOException.class, () -> FileContentFetcher.getSingleReader(badPath, Map.of()));
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
}
