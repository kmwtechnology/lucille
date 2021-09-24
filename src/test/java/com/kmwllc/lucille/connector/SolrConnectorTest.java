package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class SolrConnectorTest {

  @Test
  public void testPostActions() throws ConnectorException {
    Config config = ConfigFactory.load("SolrConnectorTest/config.conf");
    Connector connector = new SolrConnector(config);

    connector.preExecute("run");
    connector.postExecute("run");
  }

}
