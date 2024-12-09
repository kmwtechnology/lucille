package com.kmwllc.lucille.core;

import com.kmwllc.lucille.connector.CSVConnector;
import com.kmwllc.lucille.connector.JSONConnector;
import com.kmwllc.lucille.connector.xml.XMLConnector;
import com.typesafe.config.Config;

// TODO: make sure that if provided with different config, it will return different instances. Fields will be lists/Maps of Connectors instead
public class FileHandlerManager {
  private static JSONConnector jsonConnectorInstance = null;
  private static CSVConnector csvConnectorInstance = null;
  private static XMLConnector xmlConnectorInstance = null;

  private FileHandlerManager() {
  }

  public static synchronized FileHandler getJsonConnector(Config config) {
    if (jsonConnectorInstance == null) {
      jsonConnectorInstance = new JSONConnector(config);
    }
    return jsonConnectorInstance;
  }

  public static synchronized FileHandler getCsvConnector(Config config) {
    if (csvConnectorInstance == null) {
      csvConnectorInstance = new CSVConnector(config);
    }
    return csvConnectorInstance;
  }

  public static synchronized FileHandler getXmlConnector(Config config) {
    if (xmlConnectorInstance == null) {
      xmlConnectorInstance = new XMLConnector(config);
    }
    return xmlConnectorInstance;
  }

  public static synchronized void close() {
    if (jsonConnectorInstance != null) {
      jsonConnectorInstance = null;
    }

    if (csvConnectorInstance != null) {
      csvConnectorInstance = null;
    }

    if (xmlConnectorInstance != null) {
      xmlConnectorInstance = null;
    }
  }
}
