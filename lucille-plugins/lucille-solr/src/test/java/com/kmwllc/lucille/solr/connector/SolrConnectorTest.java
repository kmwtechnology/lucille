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
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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
    Date created = new Date(1_700_000_000_000L);
    SolrDocument solrDoc = new SolrDocument();
    // a numeric unique key
    solrDoc.setField(Document.ID_FIELD, 42L);
    solrDoc.setField("_version_", 1234567890123L);
    solrDoc.setField("devId", 7);
    solrDoc.setField("price", 9.5);
    solrDoc.setField("rating", 4.5f);
    solrDoc.setField("thumbnail", new byte[] {1, 2, 3});
    solrDoc.setField("inStock", true);
    solrDoc.setField("created", created);
    solrDoc.setField("Tags", List.of("a", "b"));
    // a type a Document cannot hold, such as an enum field's value, keeps its string form
    solrDoc.setField("status", new EnumFieldValue(1, "active"));

    Document doc = publishOne(ConfigFactory.empty(), solrDoc);

    assertEquals("42", doc.getId());
    // compare the stored JSON nodes, not the typed getters: getInt, getLong, getDouble and getBoolean also parse a
    // string, so they would pass if the values were copied as strings
    assertEquals(LongNode.valueOf(1234567890123L), doc.getJson("_version_"));
    // field names keep their case by default
    assertEquals(IntNode.valueOf(7), doc.getJson("devId"));
    assertEquals(DoubleNode.valueOf(9.5), doc.getJson("price"));
    assertEquals(FloatNode.valueOf(4.5f), doc.getJson("rating"));
    assertEquals(BinaryNode.valueOf(new byte[] {1, 2, 3}), doc.getJson("thumbnail"));
    assertEquals(BooleanNode.TRUE, doc.getJson("inStock"));
    // a Document stores a Date as an ISO-8601 instant string
    assertEquals(TextNode.valueOf(created.toInstant().toString()), doc.getJson("created"));
    assertEquals(created.toInstant(), doc.getInstant("created"));
    assertEquals(List.of("a", "b"), doc.getStringList("Tags"));
    assertEquals(TextNode.valueOf("active"), doc.getJson("status"));
    assertFalse(doc.has("devid"));
    assertFalse(doc.has("instock"));
    assertFalse(doc.has("tags"));
  }

  @Test
  public void testLowercaseFields() throws Exception {
    SolrDocument solrDoc = new SolrDocument();
    solrDoc.setField(Document.ID_FIELD, "doc1");
    solrDoc.setField("devId", 7);
    solrDoc.setField("Tags", List.of("a", "b"));
    solrDoc.setField("name", "n1");

    Document doc = publishOne(lowercase(true), solrDoc);

    // the field is read by its own name; only the Document's field name is lower-cased, and the value keeps its type
    assertEquals(IntNode.valueOf(7), doc.getJson("devid"));
    assertEquals(List.of("a", "b"), doc.getStringList("tags"));
    assertEquals("n1", doc.getString("name"));
    assertFalse(doc.has("devId"));
    assertFalse(doc.has("Tags"));
  }

  @Test
  public void testLowercaseFieldsFalseIsTheDefault() throws Exception {
    SolrDocument solrDoc = new SolrDocument();
    solrDoc.setField(Document.ID_FIELD, "doc1");
    solrDoc.setField("devId", 7);

    Document doc = publishOne(lowercase(false), solrDoc);

    assertEquals(IntNode.valueOf(7), doc.getJson("devId"));
    assertFalse(doc.has("devid"));
  }

  @Test
  public void testFieldsDifferingOnlyInCase() throws Exception {
    SolrDocument solrDoc = new SolrDocument();
    solrDoc.setField(Document.ID_FIELD, "doc1");
    solrDoc.setField("Author", "upper");
    solrDoc.setField("author", "lower");

    // kept apart by default
    Document doc = publishOne(ConfigFactory.empty(), solrDoc);
    assertEquals("upper", doc.getString("Author"));
    assertEquals("lower", doc.getString("author"));

    // lower-cased, they share a name and the later field overwrites the earlier
    doc = publishOne(lowercase(true), solrDoc);
    assertFalse(doc.has("Author"));
    assertEquals(List.of("lower"), doc.getStringList("author"));
  }

  @Test
  public void testMixedCaseIdFieldIsNotCopied() throws Exception {
    SolrDocument solrDoc = new SolrDocument();
    solrDoc.setField("docKey", "k1");
    solrDoc.setField("name", "n1");

    for (boolean lowercaseFields : new boolean[] {false, true}) {
      Document doc = publishOne(lowercase(lowercaseFields).withValue("idField", ConfigValueFactory.fromAnyRef("docKey")),
          solrDoc);

      assertEquals("k1", doc.getId());
      assertFalse(doc.has("docKey"));
      assertFalse(doc.has("dockey"));
      assertEquals("n1", doc.getString("name"));
    }
  }

  @Test
  public void testReservedFieldNamesAreNotCopied() throws Exception {
    SolrDocument solrDoc = reservedFieldsDoc();

    for (boolean lowercaseFields : new boolean[] {false, true}) {
      Document doc = publishOne(lowercase(lowercaseFields), solrDoc);

      // the exact reserved names are never copied, whatever the setting
      assertEquals("doc1", doc.getId());
      assertEquals("run", doc.getRunId());
      assertFalse(doc.isDropped());
      assertFalse(doc.hasChildren());
      assertEquals("n1", doc.getString("name"));
    }
  }

  @Test
  public void testMixedCaseReservedFieldNamesAreCopiedWhenCaseIsKept() throws Exception {
    Document doc = publishOne(ConfigFactory.empty(), reservedFieldsDoc());

    // the reserved names are lower case, so these are ordinary fields when their case is kept
    assertEquals("other", doc.getString("ID"));
    assertEquals("otherRun", doc.getString("RUN_ID"));
    assertEquals("yes", doc.getString("___DROPPED"));
    assertEquals("doc1", doc.getId());
    assertEquals("run", doc.getRunId());
    assertFalse(doc.isDropped());
  }

  @Test
  public void testMixedCaseReservedFieldNamesAreSkippedWhenLowercased() throws Exception {
    Document doc = publishOne(lowercase(true), reservedFieldsDoc());

    // lower-cased, these become reserved names, so they are skipped rather than failing the document
    assertEquals("doc1", doc.getId());
    assertEquals("run", doc.getRunId());
    assertFalse(doc.isDropped());
    assertFalse(doc.has("ID"));
    assertFalse(doc.has("RUN_ID"));
    assertFalse(doc.has("___DROPPED"));
    assertEquals("n1", doc.getString("name"));
  }

  @Test
  public void testLowercaseFieldsMustBeBoolean() {
    Config config = ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf")
        .withValue("lowercaseFields", ConfigValueFactory.fromAnyRef(List.of("yes")));
    Exception e = assertThrows(IllegalArgumentException.class, () -> new SolrConnector(config, mock(SolrClient.class)));
    // rejected for its type, not as an unknown property
    assertTrue(e.getMessage(), e.getMessage().contains("lowercaseFields must be a boolean"));
  }

  // a Solr document carrying each of Lucille's reserved field names, exactly and in upper case
  private static SolrDocument reservedFieldsDoc() {
    SolrDocument solrDoc = new SolrDocument();
    solrDoc.setField(Document.ID_FIELD, "doc1");
    solrDoc.setField("ID", "other");
    solrDoc.setField(Document.RUNID_FIELD, "oldRun");
    solrDoc.setField("RUN_ID", "otherRun");
    solrDoc.setField(Document.DROP_FIELD, "yes");
    solrDoc.setField("___DROPPED", "yes");
    solrDoc.setField(Document.CHILDREN_FIELD, "child");
    solrDoc.setField(Document.SKIP_FIELD, "yes");
    solrDoc.setField("name", "n1");
    return solrDoc;
  }

  private static Config lowercase(boolean lowercaseFields) {
    return ConfigFactory.empty().withValue("lowercaseFields", ConfigValueFactory.fromAnyRef(lowercaseFields));
  }

  // runs the connector, with these settings over execute.conf, against a Solr that returns just this document
  private static Document publishOne(Config overrides, SolrDocument solrDoc) throws Exception {
    Config config = overrides.withFallback(ConfigFactory.parseResourcesAnySyntax("SolrConnectorTest/execute.conf"));
    SolrClient mockClient = mock(SolrClient.class);
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    when(mockClient.query(any(SolrQuery.class))).then(invocation -> {
      MockQueryResponse resp = new MockQueryResponse();
      resp.addToResults(solrDoc);
      return resp;
    });

    new SolrConnector(config, mockClient).execute(publisher);

    // the mock's cursor moves once, so its page is served, and the document published, twice
    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(2, docs.size());
    assertEquals(docs.get(0), docs.get(1));
    return docs.get(0);
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
