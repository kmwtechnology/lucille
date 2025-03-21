package com.kmwllc.lucille.connector.jdbc;

/**
 * Represents the state of a Connector.
 */
public enum ConnectorState {
  /** A Connector that is running. */
  RUNNING,
  /** A Connector that encountered an error. */
  ERROR,
  /** A Connector that stopped. */
  STOPPED
}
