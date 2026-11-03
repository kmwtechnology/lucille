package com.kmwllc.lucille.core;

import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.LoggerFactory;

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
   * Get the name of this connector. Connectors within a Run must have unique names.
   * @return the name of the Connector as specified in the configuration.
   */
  String getName();

  /**
   * Get the name of the pipeline that this Connector will send documents to.
   * @return the name of the pipeline to which this Connector's Documents should be sent.
   */
  String getPipelineName();

  /**
   * Return whether this Connector requires a collapsing Publisher. A collapsing Publisher combines Documents that are
   * published in sequence and share the same ID. Such sequences of Documents are combined
   * into a single document with multi-valued fields.
   *
   * @return whether this Connector instance expects to be passed a collapsing Publisher when execute() is called.
   */
  boolean requiresCollapsingPublisher();

  /**
   * Performs any logic that should occur before execute().
   *
   * @param runId The id of the run you are pre-executing.
   * @throws ConnectorException in the event an error occurs.
   */
  void preExecute(String runId) throws ConnectorException;

  /**
   * Performs any data-source specific logic for connecting to a backend source,
   * acquiring data, and generating documents from it. All generated documents should be published
   * via the supplied Publisher. Connections should be closed in close().
   *
   * Will not be called if preExecute() throws an exception.
   *
   * The run ID is passed to preExecute(); a Connector that needs it during execute() should keep it from there.
   *
   * @param publisher An object that provides a publish() method accepting a document to be published.
   * @throws ConnectorException in the event an error occurs.
   */
  void execute(Publisher publisher) throws ConnectorException;

  /**
   * Performs any logic that should occur after execute(), not including
   * the closing of connections, which should be done in close().
   *
   * Will not be called if preExecute() or execute() throw an exception.
   *
   * @param runId The id of the run you are post-executing.
   * @throws ConnectorException in the event an error occurs.
   */
  void postExecute(String runId) throws ConnectorException;

  /**
   * @return The Spec describing this Connector's Config properties. Must be declared as <code>public static final Spec SPEC</code>.
   * @throws RuntimeException If this Connector does not have a <code>public static final Spec SPEC</code>.
   */
  Spec getSpec();

  /**
   * Instantiates a list of Connectors from the designated Config. A connector without a <code>name</code> is named
   * <code>connector_N</code>, where N is its 1-based position in <code>connectors</code>.
   *
   * @param config The configuration you want to extract Connectors from.
   * @throws ConnectorException If two connectors share a name, a connector class cannot be found or instantiated, or a
   *     connector's constructor throws a ConnectorException or another checked exception.
   * @throws IllegalArgumentException If a connector config names no class or a class that is not a Connector, or if a
   *     Connector's constructor rejects its config.
   * @return A list of Connectors build from the given Config.
   */
  static List<Connector> fromConfig(Config config) throws ConnectorException {
    List<? extends Config> connectorConfigs = config.getConfigList("connectors");

    List<Connector> connectors = new ArrayList<>();
    int position = 1;
    for (Config c : connectorConfigs) {
      Config named = withDefaultName(c, position);
      String name = named.getString("name");
      if (connectors.stream().anyMatch(x -> name.equals(x.getName()))) {
        throw new ConnectorException("Two connectors cannot share the same name: " + name);
      }
      connectors.add(newInstance(named));
      position++;
    }
    return connectors;
  }

  /**
   * Gets a List of Exceptions associated with the given configuration for a connector. Use to get the invalid / missing
   * properties in the given Config. A connector without a <code>name</code> is validated under the name it would have in
   * a run, <code>connector_N</code>.
   *
   * @param connectorConfig Configuration for the Connector that you want to get Exceptions for.
   * @param position The connector's 1-based position in <code>connectors</code>.
   * @return A List of Exceptions associated with the given Connector (primarily to do with whether the Configuration is
   *     valid).
   */
  static List<Exception> getConnectorConfigExceptions(Config connectorConfig, int position) {
    // We still use a list so we can support adding extra exceptions in the event of duplicate connector names,
    // even though we will only ever add one exception in the body of this method!
    List<Exception> exceptionList = new ArrayList<>();

    // Creates the connector; AbstractConnector runs validation and throws an exception as needed. The connector is
    // constructed rather than only its Spec checked, because a constructor can reject combinations the Spec cannot
    // express (such as FileConnector's collapse with several threads).
    Connector connector = null;
    try {
      connector = newInstance(withDefaultName(connectorConfig, position));
    } catch (Exception e) {
      exceptionList.add(e);
    } finally {
      // the connector was constructed only to validate its config; release any client its constructor opened
      if (connector != null) {
        try {
          connector.close();
        } catch (Exception e) {
          LoggerFactory.getLogger(Connector.class).debug("Error closing connector constructed for validation", e);
        }
      }
    }

    return exceptionList;
  }

  // the name a connector has in a run: its configured name, or connector_N for the one at 1-based position N
  // (Runner.validateConnectors applies the same rule to key its validation errors)
  private static Config withDefaultName(Config connectorConfig, int position) {
    return connectorConfig.hasPath("name")
        ? connectorConfig
        : connectorConfig.withValue("name", ConfigValueFactory.fromAnyRef("connector_" + position));
  }

  private static Connector newInstance(Config config) throws ConnectorException {
    // checked first so validation reports a clear message rather than the Config library's missing-path error
    if (!config.hasPath("class")) {
      throw new IllegalArgumentException("No Connector class specified.");
    }
    String className = config.getString("class");

    Class<?> clazz;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new ConnectorException("Connector class not found: " + className, e);
    }
    if (!Connector.class.isAssignableFrom(clazz)) {
      throw new IllegalArgumentException("Class " + className + " is not a Connector");
    }

    Constructor<?> constructor;
    try {
      constructor = clazz.getConstructor(Config.class);
    } catch (NoSuchMethodException e) {
      throw new ConnectorException("Connector class " + className + " has no public constructor taking a Config", e);
    }

    try {
      return (Connector) constructor.newInstance(config);
    } catch (InvocationTargetException e) {
      // rethrow what the constructor threw, so a validation error reaches the caller unwrapped
      Throwable cause = e.getCause();
      if (cause instanceof ConnectorException connectorException) {
        throw connectorException;
      }
      if (cause instanceof RuntimeException runtimeException) {
        throw runtimeException;
      }
      if (cause instanceof Error error) {
        throw error;
      }
      throw new ConnectorException("Error creating connector " + className, cause);
    } catch (InstantiationException | IllegalAccessException e) {
      throw new ConnectorException("Could not instantiate connector class " + className, e);
    }
  }

  /**
   * @return A message that should be included in the Lucille Run Summary for this connector instance.
   */
  String getMessage();

}
