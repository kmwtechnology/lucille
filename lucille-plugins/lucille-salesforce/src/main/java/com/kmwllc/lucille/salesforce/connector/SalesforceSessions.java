package com.kmwllc.lucille.salesforce.connector;

import com.typesafe.config.Config;

/**
 * Opens a Salesforce session from the same auth settings the connector accepts.
 */
public final class SalesforceSessions {

  private SalesforceSessions() {
  }

  public static SalesforceBlobClient open(Config config) {
    return SalesforceAuth.from(config).open(2000, false);
  }
}
