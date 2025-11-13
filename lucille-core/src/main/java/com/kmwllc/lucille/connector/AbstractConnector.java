package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

/**
 * Base class for use by Connector implementations, providing basic Config parsing behavior
 * for obtaining connector name, pipeline name, doc ID prefix, and collapsing mode.
 *
 * <p> All implementations must declare a <code>public static Spec SPEC</code> defining the Connector's properties. This Spec will be accessed
 * reflectively in the super constructor, so the Connector will not function without declaring a Spec. The Config provided
 * to <code>super()</code> will be validated against the Spec. Validation errors will reference the Connector's <code>name</code>.
 *
 * <p> A {@link SpecBuilder#connector()} always has "name", "class", "pipeline", "docIdPrefix", and "collapse" as legal properties.
 *
 * <p> Base Config Parameters:
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

  public AbstractConnector(Config config) {
    this.config = config;
    this.name = config.hasPath("name") ? config.getString("name") : "noname";
    this.pipelineName = config.hasPath("pipeline") ? config.getString("pipeline") : null;
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
    this.collapse = config.hasPath("collapse") ? config.getBoolean("collapse") : false;

    getSpec().validate(config, name);
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

  @Override
  public Spec getSpec() {
    try {
      return (Spec) this.getClass().getDeclaredField("SPEC").get(null);
    } catch (Exception e) {
      throw new RuntimeException("Error accessing " + getClass() + " Spec. Is it publicly and statically available under \"SPEC\"?", e);
    }
  }
}
