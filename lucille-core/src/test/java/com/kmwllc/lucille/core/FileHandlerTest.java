package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kmwllc.lucille.core.fileHandlers.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandlers.FileHandler;
import com.kmwllc.lucille.core.fileHandlers.FileHandlerManager;
import com.kmwllc.lucille.core.fileHandlers.JsonFileHandler;
import com.kmwllc.lucille.core.fileHandlers.XMLFileHandler;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class FileHandlerTest {

  @Test
  public void testSupportsFileType() throws Exception {
    // json file
    String jsonExtension = "json";
    assertTrue(FileHandler.supportsFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileHandler.supportsFileType(jsonExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(FileHandler.supportsFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileHandler.supportsFileType(jsonExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // jsonl file
    String jsonlExtension = "jsonl";
    assertTrue(FileHandler.supportsFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileHandler.supportsFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(FileHandler.supportsFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileHandler.supportsFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // csv file
    String csvExtension = "csv";
    assertTrue(FileHandler.supportsFileType(csvExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    assertFalse(FileHandler.supportsFileType(csvExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    // xml file
    String xmlExtension = "xml";
    assertTrue(FileHandler.supportsFileType(xmlExtension, ConfigFactory.parseMap(Map.of("xml", "content"))));
    assertFalse(FileHandler.supportsFileType(xmlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
  }

  @Test
  public void testGetFileHandler() throws Exception {
    // json handler
    String jsonExtension = "json";
    FileHandler jsonHandler = FileHandler.getFileHandler(jsonExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // jsonl handler using jsonl
    String jsonlExtension = "jsonl";
    FileHandler jsonlHandler = FileHandler.getFileHandler(jsonlExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileHandler.class, jsonlHandler);
    // validate that the same handler is given if the config contents are the same
    assertEquals(jsonHandler, jsonlHandler);

    // validate that a different handler is given if the config contents are the different
    FileHandler jsonlHandler2 = FileHandler.getFileHandler(jsonlExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value2"))));
    assertNotEquals(jsonlHandler, jsonlHandler2);

    // csv handler
    String csvExtension = "csv";
    FileHandler csvHandler = FileHandler.getFileHandler(csvExtension,
        ConfigFactory.parseMap(Map.of("csv", Map.of("csvKey", "csvValue"))));
    assertInstanceOf(CSVFileHandler.class, csvHandler);

    // xml handler
    String xmlExtension = "xml";
    FileHandler xmlHandler = FileHandler.getFileHandler(xmlExtension,
        ConfigFactory.parseMap(Map.of("xml", Map.of("xmlRootPath", "path", "xmlIdPath", "idPath"))));
    assertInstanceOf(XMLFileHandler.class, xmlHandler);

    FileHandlerManager.closeAllHandlers();
  }
}
