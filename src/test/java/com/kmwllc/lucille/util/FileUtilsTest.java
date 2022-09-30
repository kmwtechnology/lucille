package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.Reader;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

public class FileUtilsTest {
  @Test
  public void getReaderTest() throws Exception {
    String[] paths = {
      "classpath:FileUtilsTest/test",
      "gz:" + new File("src/test/resources/FileUtilsTest/test.gz").toURI(),
      "src/test/resources/FileUtilsTest/test"
    };
    for (String path : paths) {
      try (Reader r = FileUtils.getReader(path)) {
        assertEquals("aaaaa", IOUtils.toString(r));
      }
    }
  }
}
