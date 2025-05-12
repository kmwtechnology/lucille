package com.kmwllc.lucille.core.fileHandler;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
    Config jsonConfig = ConfigFactory.parseMap(Map.of("json", Map.of()));
    FileHandler jsonHandler = FileHandler.create("json", jsonConfig);
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // test jsonl
    jsonConfig = ConfigFactory.parseMap(Map.of("json", Map.of()));
    jsonHandler = FileHandler.create("json", jsonConfig);
    assertInstanceOf(JsonFileHandler.class, jsonHandler);
    // test unsupported file type
    assertThrows(ConfigException.class, () -> FileHandler.create("unsupported", ConfigFactory.parseMap(Map.of())));

    Config customHandler = ConfigFactory.parseMap(Map.of(
        "txt", Map.of(
            "class", "com.jakesq24.fileHandler.TextFileHandler",
            "docIdPrefix", "text-handled"
        )));
    // reflective error will occur... this TextFileHandler isn't anywhere.
    assertThrows(IllegalArgumentException.class, () -> FileHandler.create("txt", customHandler));

    // A bit weird, but an easy way to make sure reflection works
    Config withCSVOnOtherType = ConfigFactory.parseMap(Map.of(
        "tsv", Map.of(
            "class", "com.kmwllc.lucille.core.fileHandler.CSVFileHandler",
            "useTabs", true
        )));
    assertInstanceOf(CSVFileHandler.class, FileHandler.create("tsv", withCSVOnOtherType));
  }


  @Test
  public void testCreateFromConfig() {

  }
}
