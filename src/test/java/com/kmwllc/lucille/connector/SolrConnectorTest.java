package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

public class SolrConnectorTest {

  @Test
  public void testActions() throws ConnectorException, SolrServerException, IOException {
    Config config = ConfigFactory.load("SolrConnectorTest/config.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenReturn(new NamedList<>());
    Connector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run");
    connector.postExecute("run");

    // We expect the request method to be called 5 times, since we supplied 2 pre actions and 3 post actions.
    verify(mockClient, times(5)).request(any(GenericSolrRequest.class));
  }

  @Test(expected = ConnectorException.class)
  public void testFailingActions() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.load("SolrConnectorTest/config.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenThrow();
    Connector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run");
    connector.postExecute("run");
  }

  @Test
  public void testRunIdReplacement() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.load("SolrConnectorTest/runId.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenReturn(new NamedList<>());
    SolrConnector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run1");
    assertEquals("<delete><query>runId:run1</query></delete>", connector.getLastExecutedPreActions().get(0));
  }
}