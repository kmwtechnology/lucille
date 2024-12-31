package com.kmwllc.lucille.core.fileHandler;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import org.junit.Test;

public class FileHandlerTest {

  @Test
  public void testCreate() {
    // test csv
    Config csvConfig = ConfigFactory.parseMap(Map.of("csv", Map.of()));
    FileHandler csvHandler = FileHandler.create("csv", csvConfig);
    assertInstanceOf(CSVFileHandler.class, csvHandler);
    // test xml
    Config xmlConfig = ConfigFactory.parseMap(Map.of("xml", Map.of("xmlRootPath", "path", "xmlIdPath", "idPath")));
    FileHandler xmlHandler = FileHandler.create("xml", xmlConfig);
    assertInstanceOf(XMLFileHandler.class, xmlHandler);
    // test xml without required configs
    assertThrows(ConfigException.class, () -> FileHandler.create("xml", ConfigFactory.parseMap(Map.of("xml", Map.of()))));
    // test json
    Config jsonConfig = ConfigFactory.parseMap(Map.of("jsonl", Map.of()));
    FileHandler jsonHandler = FileHandler.create("json", jsonConfig);
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // test jsonl
    jsonConfig = ConfigFactory.parseMap(Map.of("json", Map.of()));
    jsonHandler = FileHandler.create("jsonl", jsonConfig);
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // test unsupported file type
    assertThrows(UnsupportedOperationException.class, () -> FileHandler.create("unsupported", ConfigFactory.parseMap(Map.of())));
  }


  @Test
  public void testSupportAndContainFileType() {
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

    // test unsupported file type
    assertFalse(FileHandler.supportAndContainFileType("unsupported", ConfigFactory.parseMap(Map.of("unsupported", "content"))));
  }
}
