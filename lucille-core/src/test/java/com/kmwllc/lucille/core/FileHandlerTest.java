package com.kmwllc.lucille.core;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.FileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.kmwllc.lucille.core.fileHandler.XMLFileHandler;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class FileHandlerTest {

  @Test
  public void testSupportAndContainFileType() throws Exception {
    // json file
    String jsonExtension = "json";
    assertTrue(FileHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(
        FileHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileHandler.supportAndContainFileType(jsonExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // jsonl file
    String jsonlExtension = "jsonl";
    assertTrue(FileHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    assertTrue(FileHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("jsonl", "content"))));
    assertTrue(
        FileHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("json", "content", "jsonl", "content"))));
    assertFalse(FileHandler.supportAndContainFileType(jsonlExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    // csv file
    String csvExtension = "csv";
    assertTrue(FileHandler.supportAndContainFileType(csvExtension, ConfigFactory.parseMap(Map.of("csv", "content"))));
    assertFalse(FileHandler.supportAndContainFileType(csvExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
    // xml file
    String xmlExtension = "xml";
    assertTrue(FileHandler.supportAndContainFileType(xmlExtension, ConfigFactory.parseMap(Map.of("xml", "content"))));
    assertFalse(FileHandler.supportAndContainFileType(xmlExtension, ConfigFactory.parseMap(Map.of("json", "content"))));
  }

  @Test
  public void testCreate() throws Exception {
    // json handler
    String jsonExtension = "json";
    FileHandler jsonHandler = FileHandler.create(jsonExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // jsonl handler using jsonl
    String jsonlExtension = "jsonl";
    FileHandler jsonlHandler = FileHandler.create(jsonlExtension,
        ConfigFactory.parseMap(Map.of("json", Map.of("key", "value"))));
    assertInstanceOf(JsonFileHandler.class, jsonlHandler);

    // validate that a different handler is given
    assertNotEquals(jsonHandler, jsonlHandler);

    // csv handler
    String csvExtension = "csv";
    FileHandler csvHandler = FileHandler.create(csvExtension,
        ConfigFactory.parseMap(Map.of("csv", Map.of("csvKey", "csvValue"))));
    assertInstanceOf(CSVFileHandler.class, csvHandler);

    // xml handler
    String xmlExtension = "xml";
    FileHandler xmlHandler = FileHandler.create(xmlExtension,
        ConfigFactory.parseMap(Map.of("xml", Map.of("xmlRootPath", "path", "xmlIdPath", "idPath"))));
    assertInstanceOf(XMLFileHandler.class, xmlHandler);
  }
}
