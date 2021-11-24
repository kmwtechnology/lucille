package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;

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
 * A new Connector instance will be created for each run.
 * During a run:
 *      preExecute() is always called
 *      execute() is called if preExecute() succeeds
 *      postExecute() is called if preExecute() and execute() succeed
 *      close() is always called
 */
public interface Connector extends AutoCloseable {

  String getName();

  String getPipelineName();

  boolean requiresCollapsingPublisher();

  /**
   * Performs any logic that should occur before execute().
   */
  void preExecute(String runId) throws ConnectorException;

  /**
   * Performs any data-source specific logic for connecting to a backend source,
   * acquiring data, and generating documents from it. All generated documents should be published
   * via the supplied Publisher. Connections should be closed in close().
   *
   * Will not be called if preExecute() throws an exception.
   *
   * @param publisher provides a publish() method accepting a document to be published
   */
  void execute(Publisher publisher) throws ConnectorException;

  /**
   * Performs any logic that should occur after execute(), not including
   * the closing of connections, which should be done in close().
   *
   * Will not be called if preExecute() or execute() throw an exception.
   */
  void postExecute(String runId) throws ConnectorException;

  /**
   * Instantiates a list of Connectors from the designated Config.
   */
  static List<Connector> fromConfig(Config config) throws ClassNotFoundException, NoSuchMethodException,
    IllegalAccessException, InvocationTargetException, InstantiationException, ConnectorException {
    List<? extends Config> connectorConfigs = config.getConfigList("connectors");

    List<Connector> connectors = new ArrayList();
    int index = 1;
    for (Config c : connectorConfigs) {
      final String name = c.hasPath("name") ? c.getString("name") : ("connector_" + index);
      if (!c.hasPath("name")) {
        c = c.withValue("name", ConfigValueFactory.fromAnyRef(name));
      }
      if (connectors.stream().anyMatch(x -> name.equals(x.getName()))) {
        throw new ConnectorException("Two connectors cannot share the same name: " + name);
      }

      Class<?> clazz = Class.forName(c.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);
      Connector connector = (Connector) constructor.newInstance(c);
      connectors.add(connector);
      index++;
    }
    return connectors;
  }

}
