package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;

/**
 * Base class for use by Connector implementations, providing basic Config parsing behavior
 * for obtaining connector name, pipeline name, doc ID prefix, and collapsing mode.
 */
public abstract class AbstractConnector implements Connector {

  private String name;
  private String pipelineName;
  private String docIdPrefix;
  private boolean collapse;
  private String message = null;

  /**
   * The configuration for this Connector.
   */
  protected final Config config;

  /**
   * Create an abstract connector from the given Config.
   * @param config The configuration of the connector. Must have a name.
   */
  public AbstractConnector(Config config) {
    this.config = config;
    this.name = config.getString("name");
    this.pipelineName = config.hasPath("pipeline") ? config.getString("pipeline") : null;
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
    this.collapse = config.hasPath("collapse") ? config.getBoolean("collapse") : false;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public String getPipelineName() {
    return pipelineName;
  }

  @Override
  public boolean requiresCollapsingPublisher() {
    return collapse;
  }

  @Override
  public void postExecute(String runId) throws ConnectorException {
    // no-op
  }

  @Override
  public void preExecute(String runId) throws ConnectorException {
    // no-op
  }

  @Override
  public void close() throws ConnectorException {
    // no-op
  }

  /**
   * Returns the configured prefix that this Connector will prepend to ids from the source
   * data when creating Documents from that data.
   * @return The prefix that this Connector will append to document IDs when created.
   */
  public String getDocIdPrefix() {
    return docIdPrefix;
  }

  /**
   * Creates an extended doc ID by adding a prefix (and possibly in the future, a suffix) to the
   * given id.
   * @param id An id to use and, potentially, add a prefix to.
   * @return A completed doc ID, including the prefix and the given id.
   */
  public String createDocId(String id) {
    return docIdPrefix + id;
  }

  @Override
  public String getMessage() {
    return message;
  }

  /**
   * Sets the message associated with this connector.
   * @param message The new message to associate with this connector.
   */
  protected void setMessage(String message) {
    this.message = message;
  }

}
