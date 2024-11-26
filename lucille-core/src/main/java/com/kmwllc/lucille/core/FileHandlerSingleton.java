package com.kmwllc.lucille.core;

import com.kmwllc.lucille.connector.JSONConnector;
import com.typesafe.config.Config;

public class FileHandlerSingleton {
  private static JSONConnector JsonConnectorInstance = null;


  private FileHandlerSingleton() {
  }

  public static synchronized FileHandler getJsonConnectorInstance(Config config) {
    if (JsonConnectorInstance == null) {
      JsonConnectorInstance = new JSONConnector(config);
    }
    return JsonConnectorInstance;
  }

  public static synchronized void closeAllHandlers() {
    if (JsonConnectorInstance != null) {
      JsonConnectorInstance = null;
    }
  }
}
