package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Spec;
import com.typesafe.config.Config;

/**
 * Base class for use by Connector implementations, providing basic Config parsing behavior
 * for obtaining connector name, pipeline name, doc ID prefix, and collapsing mode.
 *
 * All Connectors will have their configuration validated by {@link Spec#validate(Config, String)}. In the Connector's constructor,
 * define the required/optional properties/parents in a {@link Spec#connector()}. Validation errors will mention the connector's <code>name</code>.
 *
 * <br> A {@link Spec#connector()} always has "name", "class", "pipeline", "docIdPrefix", and "collapse" as legal properties.
 *
 * <br> Base Config Parameters:
 * <ul>
 *   <li>name (String): The name of the Connector. Connector names should be unique (within your Lucille Config).</li>
 *   <li>class (String): The class for the Connector your want to use.</li>
 *   <li>pipeline (String, Optional): The name of the pipeline to feed Documents to. Defaults to null (no pipeline).</li>
 *   <li>docIdPrefix (String, Optional): A String to prepend to Document IDs originating from this Connector. Defaults to an empty string (no prefix).</li>
 *   <li>collapse (Boolean, Optional): Whether this Connector is "collapsing". Defaults to false.</li>
 * </ul>
 */
public abstract class AbstractConnector implements Connector {

  private String name;
  private String pipelineName;
  private String docIdPrefix;
  private boolean collapse;
  private String message = null;
  protected final Config config;

  public AbstractConnector(Config config, Spec spec) {
    this.config = config;
    this.name = config.getString("name");
    this.pipelineName = config.hasPath("pipeline") ? config.getString("pipeline") : null;
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
    this.collapse = config.hasPath("collapse") ? config.getBoolean("collapse") : false;

    spec.validate(config, name);
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
   */
  public String getDocIdPrefix() {
    return docIdPrefix;
  }

  /**
   * Creates an extended doc ID by adding a prefix (and possibly in the future, a suffix) to the
   * given id.
   */
  public String createDocId(String id) {
    return docIdPrefix + id;
  }

  @Override
  public String getMessage() {
    return message;
  }

  protected void setMessage(String message) {
    this.message = message;
  }

}
