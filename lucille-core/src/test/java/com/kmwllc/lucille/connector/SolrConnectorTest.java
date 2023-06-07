package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

public class SolrConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.load("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run", "pipeline1");

    when(mockClient.query(any(SolrQuery.class))).then(new Answer<>() {
      @Override
      public QueryResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
        SolrQuery q = invocationOnMock.getArgument(0);

        SolrDocument doc = new SolrDocument();
        doc.setField(Document.ID_FIELD, "doc");
        for (String param : q.getParameterNames()) {
          if (param.equals("cursorMark"))
            continue;
          doc.setField(param.toLowerCase(), q.getParams(param));
        }

        MockQueryResponse resp = new MockQueryResponse();
        resp.addToResults(doc);
        resp.getResults().setNumFound(3);
        return resp;
      }
    });

    Connector connector = new SolrConnector(config, mockClient);
    connector.execute(publisher);

    Document testDoc = Document.create("doc", "run");
    testDoc.update("q", UpdateMode.DEFAULT, "type:product");
    testDoc.update("fq", UpdateMode.DEFAULT, "devId:[5 TO 20]", "date:today");
    testDoc.update("fl", UpdateMode.DEFAULT, "date", "devId", "id", "name", "category");
    testDoc.update("sort", UpdateMode.DEFAULT, "devId desc", "id asc");
    testDoc.update("rows", UpdateMode.DEFAULT, "1");

    //verify(mockPublisher, times(2)).publish(testDoc);

    assertEquals(2, manager.getSavedDocumentsSentForProcessing().size());
    for (Document doc : manager.getSavedDocumentsSentForProcessing()) {
      assertEquals(testDoc, doc);
    }
  }

  @Test(expected = ConnectorException.class)
  public void testFailingExecute() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.load("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    Publisher mockPublisher = mock(Publisher.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenThrow();
    Connector connector = new SolrConnector(config, mockClient);

    connector.execute(mockPublisher);
  }

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
    connector.preExecute("run2");
    assertEquals("<delete><query>runId:run2</query></delete>", connector.getLastExecutedPreActions().get(0));

    connector.postExecute("run1");
    assertEquals("<query>runId:run1</query>", connector.getLastExecutedPostActions().get(0));
    connector.postExecute("run2");
    assertEquals("<query>runId:run2</query>", connector.getLastExecutedPostActions().get(0));
  }

  @Test
  public void testRunIdReplacementJson() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.load("SolrConnectorTest/runIdJson.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenReturn(new NamedList<>());
    SolrConnector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run1");
    assertEquals("{\"delete\":{\"query\":\"runId:run1\"}}", connector.getLastExecutedPreActions().get(0));
    connector.preExecute("run2");
    assertEquals("{\"delete\":{\"query\":\"runId:run2\"}}", connector.getLastExecutedPreActions().get(0));

    connector.postExecute("run1");
    assertEquals("{\"delete\":{\"query\":\"runId:run1\"}}", connector.getLastExecutedPostActions().get(0));
    connector.postExecute("run2");
    assertEquals("{\"delete\":{\"query\":\"runId:run2\"}}", connector.getLastExecutedPostActions().get(0));
  }
}
