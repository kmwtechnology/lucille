package com.kmwllc.lucille.util;
import java.io.IOException;
import java.nio.file.Paths;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.Reader;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class FileUtilsTest {

  @Test
  public void getLocalReaderTest() throws Exception {
    String path = "classpath:FileUtilsTest/test";
    try (Reader r = FileUtils.getLocalFileReader(path)) {
      assertEquals("aaaaa", IOUtils.toString(r));
    }

    path = "src/test/resources/FileUtilsTest/test";
    try (Reader r = FileUtils.getLocalFileReader(path)) {
      assertEquals("aaaaa", IOUtils.toString(r));
    }

    String badPath = Paths.get("src/test/resources/FileUtilsTest/test").toUri().toString();
    assertThrows(IOException.class, () -> FileUtils.getLocalFileReader(badPath));
  }
}
