package com.kmwllc.lucille.callback;

import com.typesafe.config.Config;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * Connects to a data source, reads data, and generates Documents to be processed.
 *
 * Documents should be passed to a designated Publisher to make them available for downstream processing.
 *
 * Implementations of Connector should provide a constructor that takes a Config as the only argument;
 * this allows for Connectors to be instantiated reflectively based on a configuration.
 *
 * TODO: getStatus, stop, getConfiguration, getName
 */
public interface Connector {

  /**
   * An implementation of this method should perform any data-source specific logic for connecting,
   * acquiring data, and generating documents from it. All generated documents should be published
   * via the supplied Publisher
   *
   * TODO: how to handle exceptions from the start method
   *
   * @param publisher provides a publish() method accepting a document to be published
   */
  void start(Publisher publisher);

  /**
   * Instantiates a list of Connectors from the designated Config.
   */
  static List<Connector> fromConfig(Config config) throws ClassNotFoundException, NoSuchMethodException,
    IllegalAccessException, InvocationTargetException, InstantiationException {
    List<? extends Config> connectorConfigs = config.getConfigList("connectors");
    List<Connector> connectors = new ArrayList();
    for (Config c : connectorConfigs) {
      Class<?> clazz = Class.forName(c.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      Connector connector = (Connector) constructor.newInstance(c);
      connectors.add(connector);
    }
    return connectors;
  }

}
