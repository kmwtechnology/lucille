package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrRequest;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.RequestWriter;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.mockito.internal.stubbing.defaultanswers.GloballyConfiguredAnswer;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.mockito.Mockito.*;

public class SolrConnectorTest {

  @Test
  public void testPostActions() throws ConnectorException, SolrServerException, IOException {
    Config config = ConfigFactory.load("SolrConnectorTest/config.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenReturn(new NamedList<>());
    Connector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run");
    connector.postExecute("run");

    verify(mockClient, times(5)).request(any(GenericSolrRequest.class));
  }
}