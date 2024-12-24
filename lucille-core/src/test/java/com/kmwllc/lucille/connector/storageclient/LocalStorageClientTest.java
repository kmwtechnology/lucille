package com.kmwllc.lucille.connector.storageclient;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import com.kmwllc.lucille.core.fileHandler.CSVFileHandler;
import com.kmwllc.lucille.core.fileHandler.JsonFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.URI;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class LocalStorageClientTest {

  @Test
  public void testInitFileHandlers() throws Exception{
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest"), "",
        List.of(), List.of(), Map.of(), ConfigFactory.parseMap(
            Map.of("json", Map.of(), "csv", Map.of())
    ));

    localStorageClient.init();
    // check that the file handlers are initialized, 3 in this case as json and jsonl keys are populated with same fileHandler
    assertEquals(3, localStorageClient.fileHandlers.size());
    assertInstanceOf(JsonFileHandler.class, localStorageClient.fileHandlers.get("json"));
    assertInstanceOf(JsonFileHandler.class, localStorageClient.fileHandlers.get("jsonl"));
    assertEquals(localStorageClient.fileHandlers.get("json"), localStorageClient.fileHandlers.get("jsonl"));
    assertInstanceOf(CSVFileHandler.class, localStorageClient.fileHandlers.get("csv"));
    localStorageClient.shutdown();
  }

  @Test
  public void testShutdown() throws Exception{
    LocalStorageClient localStorageClient = new LocalStorageClient(new URI("src/test/resources/StorageClientTest"), "",
        List.of(), List.of(), Map.of(), ConfigFactory.parseMap(
        Map.of("json", Map.of(), "csv", Map.of())
    ));

    localStorageClient.init();
    // check that the file handlers are initialized, 3 in this case as json and jsonl keys are populated with same fileHandler
    assertEquals(3, localStorageClient.fileHandlers.size());
    localStorageClient.shutdown();

    // check that the file handlers are cleared
    assertEquals(0, localStorageClient.fileHandlers.size());
  }
}
