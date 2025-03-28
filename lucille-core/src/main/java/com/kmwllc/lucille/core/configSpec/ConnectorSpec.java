package com.kmwllc.lucille.core.configSpec;

import java.util.Set;

/**
 * A ConfigSpec for a Connector.
 */
public class ConnectorSpec extends BaseConfigSpec {

  private static final Set<String> DEFAULT_CONNECTOR_PROPERTIES = Set.of("name", "class", "pipeline", "docIdPrefix", "collapse");

  /**
   * Creates a ConnectorSpec.
   */
  public ConnectorSpec() { super(); }

  @Override
  protected Set<String> getDefaultSpecProperties() { return DEFAULT_CONNECTOR_PROPERTIES; }
}
