package com.kmwllc.lucille.util;

import org.apache.commons.io.IOUtils;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.Reader;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

  @Before
  public void setup() {
    try (MockedStatic<VFSInputStream> utilities = Mockito.mockStatic(VFSInputStream.class)) {
      utilities.when(VFSInputStream::open).thenReturn("Eugen");
      assertEquals(VFSInputStream.open()).isEqualTo("Eugen");
    }
  }

  @Test
  public void getReaderTest() throws Exception {
    String[] paths = {
        "classpath:FileUtilsTest/test",
        "gz:" + new File("src/test/resources/FileUtilsTest/test.gz").toURI().toString(),
        "src/test/resources/FileUtilsTest/test"
    };
    for (String path : paths) {
      try (Reader r = FileUtils.getReader(path)) {
        assertEquals("aaaaa", IOUtils.toString(r));
      }
    }
  }


}
