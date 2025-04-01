package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
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

  /**
   * Returns the name of the Connector as specified in the configuration.
   * Connectors within a Run must have unique names.
   */
  String getName();

  /**
   * Returns the name of the pipeline to which this Connector's Documents should be sent.
   */
  String getPipelineName();

  /**
   * Indicates whether this Connector instance expects to be passed a collapsing Publisher
   * when execute() is called. A collapsing Publisher combines Documents that are
   * published in sequence and share the same ID. Such sequences of Documents are combined
   * into a single document with multi-valued fields.
   *
   */
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

  static List<Exception> getConnectorConfigExceptions(Config connectorConfig) {
    // We still use a list so we can support adding extra exceptions in the event of duplicate connector names,
    // even though we will only ever add one exception in the body of this method!
    List<Exception> exceptionList = new ArrayList<>();

    try {
      Class<?> clazz = Class.forName(connectorConfig.getString("class"));
      Constructor<?> constructor = clazz.getConstructor(Config.class);

      // Creates the connector, AbstractConnector will run validation and throw an exception as needed.
      constructor.newInstance(connectorConfig);
    } catch (ClassNotFoundException e) {
      exceptionList.add(new ConnectorException("Connector class not found: ", e));
    } catch (InvocationTargetException e) {
      // invalid arguments cause this exception, and we only get the actual details (what the property was) via unwrapping it like so
      exceptionList.add(new ConnectorException("Error with Connector " + connectorConfig.getString("class") + ": " + e.getCause()));
    } catch (Exception e) {
      exceptionList.add(e);
    }
    return exceptionList;
  }

  /**
   * Returns a message that should be included in the Lucille Run Summary for this connector instance.
   */
  String getMessage();

}
