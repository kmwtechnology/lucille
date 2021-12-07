package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class IndexerTest {

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
    Indexer indexer = new Indexer(config, manager, solrClient, "");
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
    Indexer indexer = new Indexer(config, manager, solrClient, "");
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
    Indexer indexer = new Indexer(config, manager, solrClient, "");
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
    Config config = ConfigFactory.load("IndexerTest/exception.conf");

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

  private static class ErroringIndexer extends Indexer {

    public ErroringIndexer(Config config, IndexerMessageManager manager, boolean bypass) {
      super(config, manager, bypass, "");
    }

    @Override
    public void sendToSolr(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to Solr are correctly handled");
    }
  }



}
