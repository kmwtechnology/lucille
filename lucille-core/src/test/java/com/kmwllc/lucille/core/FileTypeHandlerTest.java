package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kmwllc.lucille.core.fileHandlers.CSVFileTypeHandler;
import com.kmwllc.lucille.core.fileHandlers.FileTypeHandler;
import com.kmwllc.lucille.core.fileHandlers.JsonFileTypeHandler;
import com.kmwllc.lucille.core.fileHandlers.XMLFileTypeHandler;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class FileTypeHandlerTest {

  @Test
  public void testSupportAndContainFileType() throws Exception {
    // json file
    String jsonExtension = "json";
    assertTrue(FileTypeHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileTypeHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(
        FileTypeHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileTypeHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // jsonl file
    String jsonlExtension = "jsonl";
    assertTrue(FileTypeHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileTypeHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(FileTypeHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileTypeHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // csv file
    String csvExtension = "csv";
    assertTrue(FileTypeHandler.supportAndContainFileType(csvExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    assertFalse(FileTypeHandler.supportAndContainFileType(csvExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    // xml file
    String xmlExtension = "xml";
    assertTrue(FileTypeHandler.supportAndContainFileType(xmlExtension, ConfigFactory.parseMap(Map.of("xml", "content"))));
    assertFalse(FileTypeHandler.supportAndContainFileType(xmlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
  }

  @Test
  public void testGetNewFileTypeHandler() throws Exception {
    // json handler
    String jsonExtension = "json";
    FileTypeHandler jsonHandler = FileTypeHandler.getNewFileTypeHandler(jsonExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileTypeHandler.class, jsonHandler);
    // jsonl handler using jsonl
    String jsonlExtension = "jsonl";
    FileTypeHandler jsonlHandler = FileTypeHandler.getNewFileTypeHandler(jsonlExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileTypeHandler.class, jsonlHandler);

    // validate that a different handler is given
    assertNotEquals(jsonHandler, jsonlHandler);

    // csv handler
    String csvExtension = "csv";
    FileTypeHandler csvHandler = FileTypeHandler.getNewFileTypeHandler(csvExtension,
        ConfigFactory.parseMap(Map.of("csv", Map.of("csvKey", "csvValue"))));
    assertInstanceOf(CSVFileTypeHandler.class, csvHandler);

    // xml handler
    String xmlExtension = "xml";
    FileTypeHandler xmlHandler = FileTypeHandler.getNewFileTypeHandler(xmlExtension,
        ConfigFactory.parseMap(Map.of("xml", Map.of("xmlRootPath", "path", "xmlIdPath", "idPath"))));
    assertInstanceOf(XMLFileTypeHandler.class, xmlHandler);
  }
}
