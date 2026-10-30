package com.kmwllc.lucille.solr.connector;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.FloatNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.request.GenericSolrRequest;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.EnumFieldValue;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.params.CursorMarkParams;
import org.apache.solr.common.util.NamedList;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SolrConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    // the connector reuses one SolrQuery and moves its cursor, so record the cursor as each query is sent
    List<String> cursorMarksSent = new ArrayList<>();
    when(mockClient.query(any(SolrQuery.class))).then(new Answer<>() {
      @Override
      public QueryResponse answer(InvocationOnMock invocationOnMock) throws Throwable {
        SolrQuery q = invocationOnMock.getArgument(0);
        cursorMarksSent.add(q.get(CursorMarkParams.CURSOR_MARK_PARAM));

        SolrDocument doc = new SolrDocument();
        doc.setField(Document.ID_FIELD, "doc");
        for (String param : q.getParameterNames()) {
          if (param.equals("cursorMark")) {
            continue;
          }
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

    // the first query starts the cursor with "*", the value Solr expects
    verify(mockClient, times(2)).query(any(SolrQuery.class));
    assertEquals(List.of("*", "**"), cursorMarksSent);

    Document testDoc = Document.create("doc", "run");
    testDoc.update("q", UpdateMode.DEFAULT, "type:product");
    testDoc.update("fq", UpdateMode.DEFAULT, "devId:[5 TO 20]", "date:today");
    testDoc.update("fl", UpdateMode.DEFAULT, "date", "devId", "id", "name", "category");
    testDoc.update("sort", UpdateMode.DEFAULT, "devId desc", "id asc");
    testDoc.update("rows", UpdateMode.DEFAULT, "1");

    //verify(mockPublisher, times(2)).publish(testDoc);

    assertEquals(2, messenger.getDocsSentForProcessing().size());
    for (Document doc : messenger.getDocsSentForProcessing()) {
      assertEquals(testDoc, doc);
    }
  }

  @Test
  public void testDocumentsWithNullOrBlankIdsAreSkipped() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    when(mockClient.query(any(SolrQuery.class))).then(invocation -> {
      MockQueryResponse resp = new MockQueryResponse();
      for (String id : new String[] {"ok1", null, "   ", "ok2"}) {
        SolrDocument doc = new SolrDocument();
        if (id != null) {
          doc.setField(Document.ID_FIELD, id);
        }
        doc.setField("name", "name-" + id);
        resp.addToResults(doc);
      }
      return resp;
    });

    Connector connector = new SolrConnector(config, mockClient);
    // does not throw: the documents without a usable id are skipped, not treated as a failure of the connector
    connector.execute(publisher);

    // the mock serves the same page twice, so each usable document arrives twice
    List<String> ids = messenger.getDocsSentForProcessing().stream().map(Document::getId).toList();
    assertEquals(List.of("ok1", "ok2", "ok1", "ok2"), ids);
  }

  @Test
  public void testExecuteWithNonStringAndMixedCaseFields() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");
    Date created = new Date(1_700_000_000_000L);

    when(mockClient.query(any(SolrQuery.class))).then(invocation -> {
      SolrDocument doc = new SolrDocument();
      // a numeric unique key
      doc.setField(Document.ID_FIELD, 42L);
      doc.setField("_version_", 1234567890123L);
      doc.setField("devId", 7);
      doc.setField("price", 9.5);
      doc.setField("rating", 4.5f);
      doc.setField("thumbnail", new byte[] {1, 2, 3});
      doc.setField("inStock", true);
      doc.setField("created", created);
      doc.setField("Tags", List.of("a", "b"));
      // a type a Document cannot hold, such as an enum field's value, keeps its string form
      doc.setField("status", new EnumFieldValue(1, "active"));
      MockQueryResponse resp = new MockQueryResponse();
      resp.addToResults(doc);
      return resp;
    });

    new SolrConnector(config, mockClient).execute(publisher);

    Document doc = messenger.getDocsSentForProcessing().get(0);
    assertEquals("42", doc.getId());
    // compare the stored JSON nodes, not the typed getters: getInt, getLong, getDouble and getBoolean also parse a
    // string, so they would pass if the values were copied as strings
    assertEquals(LongNode.valueOf(1234567890123L), doc.getJson("_version_"));
    // the field is read by its own name; only the Document's field name is lower-cased
    assertEquals(IntNode.valueOf(7), doc.getJson("devid"));
    assertEquals(DoubleNode.valueOf(9.5), doc.getJson("price"));
    assertEquals(FloatNode.valueOf(4.5f), doc.getJson("rating"));
    assertEquals(BinaryNode.valueOf(new byte[] {1, 2, 3}), doc.getJson("thumbnail"));
    assertEquals(BooleanNode.TRUE, doc.getJson("instock"));
    // a Document stores a Date as an ISO-8601 instant string
    assertEquals(TextNode.valueOf(created.toInstant().toString()), doc.getJson("created"));
    assertEquals(created.toInstant(), doc.getInstant("created"));
    assertEquals(List.of("a", "b"), doc.getStringList("tags"));
    assertEquals(TextNode.valueOf("active"), doc.getJson("status"));
    assertFalse(doc.has("devId"));
  }

  @Test
  public void testMixedCaseIdFieldIsNotCopied() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf")
        .withValue("idField", ConfigValueFactory.fromAnyRef("docKey"));
    SolrClient mockClient = mock(SolrClient.class);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    when(mockClient.query(any(SolrQuery.class))).then(invocation -> {
      SolrDocument doc = new SolrDocument();
      doc.setField("docKey", "k1");
      doc.setField("name", "n1");
      MockQueryResponse resp = new MockQueryResponse();
      resp.addToResults(doc);
      return resp;
    });

    new SolrConnector(config, mockClient).execute(publisher);

    Document doc = messenger.getDocsSentForProcessing().get(0);
    assertEquals("k1", doc.getId());
    assertFalse(doc.has("dockey"));
    assertEquals("n1", doc.getString("name"));
  }

  @Test(expected = ConnectorException.class)
  public void testFailingExecute() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf");
    SolrClient mockClient = mock(SolrClient.class);
    Publisher mockPublisher = mock(Publisher.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenThrow();
    Connector connector = new SolrConnector(config, mockClient);

    connector.execute(mockPublisher);
  }

  @Test
  public void testActions() throws ConnectorException, SolrServerException, IOException {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/config.conf");
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
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/config.conf");
    SolrClient mockClient = mock(SolrClient.class);
    when(mockClient.request(any(GenericSolrRequest.class))).thenThrow();
    Connector connector = new SolrConnector(config, mockClient);

    connector.preExecute("run");
    connector.postExecute("run");
  }

  @Test
  public void testRunIdReplacement() throws SolrServerException, IOException, ConnectorException {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/runId.conf");
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
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/runIdJson.conf");
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
