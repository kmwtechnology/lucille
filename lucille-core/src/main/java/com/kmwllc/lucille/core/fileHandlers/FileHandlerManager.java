package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.connector.xml.XMLConnector;
import com.typesafe.config.Config;
import java.util.HashMap;
import java.util.Map;

public class FileHandlerManager {
  private static final Map<Integer, FileHandler> fileHandlers = new HashMap<>();

  private FileHandlerManager() {
  }

  public static synchronized FileHandler getJsonConnector(Config config) {
    int configHashCode = config.hashCode();
    if (!fileHandlers.containsKey(configHashCode)) {
      fileHandlers.put(configHashCode, new JsonFileHandler(config));
    }
    return fileHandlers.get(configHashCode);
  }

  public static synchronized FileHandler getCsvConnector(Config config) {
    int configHashCode = config.hashCode();
    if (!fileHandlers.containsKey(configHashCode)) {
      fileHandlers.put(configHashCode, new CSVFileHandler(config));
    }
    return fileHandlers.get(configHashCode);
  }

  public static synchronized FileHandler getXmlConnector(Config config) {
    int configHashCode = config.hashCode();
    if (!fileHandlers.containsKey(configHashCode)) {
      fileHandlers.put(configHashCode, new XMLFileHandler(config));
    }
    return fileHandlers.get(configHashCode);
  }

  public static synchronized void closeAllHandlers() {
    fileHandlers.values().forEach(FileHandler::closeHandlerResources);
    fileHandlers.clear();
  }
}
