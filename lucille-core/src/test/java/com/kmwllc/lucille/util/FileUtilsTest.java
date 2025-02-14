package com.kmwllc.lucille.util;
import com.kmwllc.lucille.connector.storageclient.LocalStorageClient;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.junit.Test;
import java.io.Reader;

import static org.junit.Assert.assertEquals;

public class FileUtilsTest {

//  @Test
//  public void getReaderTest() throws Exception {
//    String[] paths = {
//        "classpath:FileUtilsTest/test",
//        "src/test/resources/FileUtilsTest/test",
//        Paths.get("src/test/resources/FileUtilsTest/test").toUri().toString()
//    };
//    for (String path : paths) {
//      try (Reader r = FileUtils.getReader(path, Map.of("file", new LocalStorageClient()))) {
//        assertEquals("aaaaa", IOUtils.toString(r));
//      }
//    }
//  }
//
//  @Test
//  public void getInputStream() throws Exception {
//    String[] paths = {
//        "classpath:FileUtilsTest/test",
//        "src/test/resources/FileUtilsTest/test",
//        Paths.get("src/test/resources/FileUtilsTest/test").toUri().toString()
//    };
//    for (String path : paths) {
//      try (InputStream is = FileUtils.getInputStream(path, Map.of("file", new LocalStorageClient()))) {
//        assertEquals("aaaaa", new String(is.readAllBytes()));
//      }
//    }
//  }
}
