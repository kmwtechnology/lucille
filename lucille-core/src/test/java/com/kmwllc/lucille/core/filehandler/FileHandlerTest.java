package com.kmwllc.lucille.core.filehandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.util.Map;
import org.junit.Test;

public class FileHandlerTest {

  @Test
  public void testSupportsProcessFile() {
    assertTrue(FileHandler.create("csv", ConfigFactory.parseMap(Map.of("csv", Map.of()))).supportsProcessFile());
    assertTrue(FileHandler.create("json", ConfigFactory.parseMap(Map.of("json", Map.of()))).supportsProcessFile());
    FileHandler xml = FileHandler.create("xml",
        ConfigFactory.parseMap(Map.of("xml", Map.of("xmlRootPath", "path", "xmlIdPath", "idPath"))));
    assertFalse(xml.supportsProcessFile());
    assertThrows(FileHandlerException.class, () -> xml.processFile(new ByteArrayInputStream(new byte[0]), "test.xml"));
  }

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
    assertThrows(IllegalArgumentException.class, () -> FileHandler.create("xml", ConfigFactory.parseMap(Map.of("xml", Map.of()))));
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
            "class", "com.kmwllc.lucille.core.filehandler.CSVFileHandler",
            "useTabs", true
        )));
    assertInstanceOf(CSVFileHandler.class, FileHandler.create("tsv", withCSVOnOtherType));
  }

  @Test
  public void testCreateFromConfig() {
    Map<String, FileHandler> result = FileHandler.createFromConfig(ConfigFactory.empty());
    assertTrue(result.isEmpty());

    result = FileHandler.createFromConfig(ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/csv_only.conf"));
    assertEquals(1, result.size());
    assertInstanceOf(CSVFileHandler.class, result.get("csv"));

    result = FileHandler.createFromConfig(ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/all_three.conf"));
    assertEquals(4, result.size());
    assertInstanceOf(CSVFileHandler.class, result.get("csv"));
    assertInstanceOf(JsonFileHandler.class, result.get("json"));
    assertInstanceOf(JsonFileHandler.class, result.get("jsonl"));
    // small detail... when you only specify one (json, jsonl), they are the *same* JsonFileHandler.
    assertEquals(result.get("json"), result.get("jsonl"));
    assertInstanceOf(XMLFileHandler.class, result.get("xml"));

    result = FileHandler.createFromConfig(ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/csv_on_other_type.conf"));
    assertEquals(5, result.size());
    assertInstanceOf(CSVFileHandler.class, result.get("csv"));
    assertInstanceOf(JsonFileHandler.class, result.get("json"));
    assertInstanceOf(JsonFileHandler.class, result.get("jsonl"));
    assertInstanceOf(XMLFileHandler.class, result.get("xml"));
    assertInstanceOf(CSVFileHandler.class, result.get("tsv"));

    result = FileHandler.createFromConfig(ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/jsonAndJsonl.conf"));
    assertEquals(2, result.size());
    assertInstanceOf(JsonFileHandler.class, result.get("json"));
    assertInstanceOf(JsonFileHandler.class, result.get("jsonl"));
    // small detail... when you explicitly specify both, each is its own unique JsonFileHandler... in case you want to
    // handle one differently than the other.
    assertNotEquals(result.get("json"), result.get("jsonl"));

    Config missingCustom = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/missing_custom.conf");
    assertThrows(IllegalArgumentException.class, () -> FileHandler.createFromConfig(missingCustom));

    // custom file handler implementation missing "class" field triggers IllegalArgumentException
    Config missingClass = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/missing_class.conf");
    assertThrows(IllegalArgumentException.class, () -> FileHandler.createFromConfig(missingClass));

    // manually overriding csv to have a different type (in this case, a JsonFileHandler... for some reason)
    Config overrideCSV = ConfigFactory.parseResourcesAnySyntax("FileHandlerTest/override_csv.conf");
    result = FileHandler.createFromConfig(overrideCSV);
    assertInstanceOf(JsonFileHandler.class, result.get("csv"));
  }

  // fileHandlers keys are extensions, matched without regard to case
  @Test
  public void testCreateFromConfigLowerCasesKeys() {
    Map<String, FileHandler> result = FileHandler.createFromConfig(ConfigFactory.parseMap(Map.of("CSV", Map.of(), "Json", Map.of())));
    assertInstanceOf(CSVFileHandler.class, result.get("csv"));
    assertInstanceOf(JsonFileHandler.class, result.get("json"));

    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> FileHandler.createFromConfig(ConfigFactory.parseMap(Map.of("csv", Map.of(), "CSV", Map.of()))));
    assertTrue(e.getMessage().contains("case"));
  }
}
