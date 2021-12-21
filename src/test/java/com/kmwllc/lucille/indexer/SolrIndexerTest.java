package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for SolrIndexer.
 *
 * TODO: Split these tests into IndexerTest vs. SolrIndexerTest. Tests that only flex generic Indexer functionality
 * should be moved to IndexerTest even if they use a SolrIndexer as a means of invoking that functionality.
 */
public class SolrIndexerTest {

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to Solr,
   * assuming a batch size of 1.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithBatchSize1() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

    // Each batch should be sent to Solr via a call to solrClient.add()
    // So, given 2 docs and a batch size of 1, there should be 2 batches and 2 calls to add()
    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(2)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(2, captor.getAllValues().size());
    assertEquals(doc.getId(), ((SolrInputDocument)captor.getAllValues().get(0).toArray()[0]).getFieldValue("id"));
    assertEquals(doc2.getId(), ((SolrInputDocument)captor.getAllValues().get(1).toArray()[0]).getFieldValue("id"));

    assertTrue(manager.hasEvents());
    assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends them to Solr,
   * assuming a batch size of 2.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithBatchSize2() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(2));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

    // Each batch should be sent to Solr via a call to solrClient.add()
    // So, given 2 docs and a batch size of 2, there should be 1 batch and 1 call to add()
    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    assertEquals(doc.getId(),
      ((SolrInputDocument)captor.getAllValues().get(0).toArray()[0]).getFieldValue(Document.ID_FIELD));
    assertEquals(doc2.getId(),
      ((SolrInputDocument)captor.getAllValues().get(0).toArray()[1]).getFieldValue(Document.ID_FIELD));

    assertTrue(manager.hasEvents());
    assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  /**
   * Tests that the indexer correctly handles the idOverrideField config setting
   *
   * @throws Exception
   */
  @Test
  public void testIdOverride() throws Exception {
    Config config = ConfigFactory.empty().
      withValue("indexer.idOverrideField", ConfigValueFactory.fromAnyRef("id_temp")).
      withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    // idOverrideField is set to id_temp
    // doc1 and doc2 have a value for id_temp, doc2 doesn't
    Document doc = new Document("doc1", "test_run");
    doc.setField("id_temp", "doc1_overriden");
    Document doc2 = new Document("doc2", "test_run");
    Document doc3 = new Document("doc3", "test_run");
    doc3.setField("id_temp", "doc3_overriden");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    indexer.run(3);

    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(3)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(3, captor.getAllValues().size());

    // confirm that doc1 and doc3 are sent with their overriden IDs, while doc2 is sent with its actual ID
    assertEquals("doc1_overriden",
      ((SolrInputDocument)captor.getAllValues().get(0).toArray()[0]).getFieldValue("id"));
    assertEquals(doc2.getId(), ((SolrInputDocument)captor.getAllValues().get(1).toArray()[0]).getFieldValue("id"));
    assertEquals("doc3_overriden",
      ((SolrInputDocument)captor.getAllValues().get(2).toArray()[0]).getFieldValue("id"));

    assertTrue(manager.hasEvents());
    assertEquals(3, manager.getSavedEvents().size());

    // confirm that events are sent using original doc IDs, not overriden ones
    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  /**
   * Tests that the indexer correctly handles nested child documents.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithNestedChildDocs() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");
    doc2.addToField("myListField", "val1");
    doc2.addToField("myListField", "val2");
    Document doc3 = new Document("doc3", "test_run");
    doc.addChild(doc2);
    doc.addChild(doc3);
    assertTrue(doc.has(Document.CHILDREN_FIELD));

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    SolrInputDocument solrDoc = (SolrInputDocument)captor.getAllValues().get(0).toArray()[0];
    assertEquals(doc.getId(), solrDoc.getFieldValue(Document.ID_FIELD));
    assertEquals(2, solrDoc.getChildDocuments().size());
    assertEquals(doc2.getId(), solrDoc.getChildDocuments().get(0).getFieldValue(Document.ID_FIELD));
    Collection<Object> myListField = solrDoc.getChildDocuments().get(0).getFieldValues("myListField");
    assertEquals(2, myListField.size());
    assertEquals("val1", myListField.toArray()[0]);
    assertEquals("val2", myListField.toArray()[1]);
    assertEquals(doc3.getId(), solrDoc.getChildDocuments().get(1).getFieldValue(Document.ID_FIELD));
    // the solr doc should not have Document.CHILDREN_FIELD;
    // the children should have been added via solrDoc.addChildDocument
    assertFalse(solrDoc.containsKey(Document.CHILDREN_FIELD));

    assertTrue(manager.hasEvents());
    assertEquals(1, manager.getSavedEvents().size());
    List<Event> events = manager.getSavedEvents();
    assertEquals(1, events.size());
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testSolrException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("SolrIndexerTest/exception.conf");

    Document doc = new Document("doc1", "test_run");
    Document doc2 = new Document("doc2", "test_run");
    Document doc3 = new Document("doc3", "test_run");
    Document doc4 = new Document("doc4", "test_run");
    Document doc5 = new Document("doc5", "test_run");

    Indexer indexer = new ErroringIndexer(config, manager, true);
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    List<Event> events = manager.getSavedEvents();
    assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  @Test
  public void testIndexerWithNestedJson() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\":1, \"b\":2}");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = manager.getSavedEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat("Attempting to index a document with a nested object field to solr should result in an indexing failure event.",
      Event.Type.FAIL, equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithNestedJsonMultivalued() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = manager.getSavedEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat("Attempting to index a document with a nested object field to solr should result in an indexing failure event.",
      Event.Type.FAIL, equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithNestedJsonWithObjects() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();

    Document doc = new Document("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, manager, solrClient, "");
    manager.sendCompleted(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor =
      ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = manager.getSavedEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat("Attempting to index a document with a nested object field to solr should result in an indexing failure event.",
      Event.Type.FAIL, equalTo(events.get(0).getType()));
  }

  private static class ErroringIndexer extends SolrIndexer {

    public ErroringIndexer(Config config, IndexerMessageManager manager, boolean bypass) {
      super(config, manager, bypass, "");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to Solr are correctly handled");
    }
  }

}
