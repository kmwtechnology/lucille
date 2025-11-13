package com.kmwllc.lucille.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.response.UpdateResponse;
import org.apache.solr.common.SolrInputDocument;
import org.hamcrest.MatcherAssert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for SolrIndexer.
 *
 * <p>TODO: Split these tests into IndexerTest vs. SolrIndexerTest. Tests that only flex generic
 * Indexer functionality should be moved to IndexerTest even if they use a SolrIndexer as a means of
 * invoking that functionality.
 */
public class SolrIndexerTest {

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends
   * them to Solr, assuming a batch size of 1.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithBatchSize1() throws Exception {
    Config config = ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    // Each batch should be sent to Solr via a call to solrClient.add()
    // So, given 2 docs and a batch size of 1, there should be 2 batches and 2 calls to add()
    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(2)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(2, captor.getAllValues().size());
    assertEquals(doc.getId(), getCapturedID(captor, 0, 0));
    assertEquals(doc2.getId(), getCapturedID(captor, 1, 0));

    assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  /**
   * Tests that the indexer correctly polls completed documents from the destination topic and sends
   * them to Solr, assuming a batch size of 2.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithBatchSize2() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(2));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    // Each batch should be sent to Solr via a call to solrClient.add()
    // So, given 2 docs and a batch size of 2, there should be 1 batch and 1 call to add()
    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    assertEquals(doc.getId(), getCapturedID(captor, 0, 0));
    assertEquals(doc2.getId(), getCapturedID(captor, 0, 1));

    assertEquals(2, messenger.getSentEvents().size());

    List<Event> events = messenger.getSentEvents();
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
    Config config =
        ConfigFactory.empty()
            .withValue("indexer.idOverrideField", ConfigValueFactory.fromAnyRef("id_temp"))
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    // idOverrideField is set to id_temp
    // doc1 and doc2 have a value for id_temp, doc2 doesn't
    Document doc = Document.create("doc1", "test_run");
    doc.setField("id_temp", "doc1_overriden");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("id_temp", "doc3_overriden");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(3)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(3, captor.getAllValues().size());

    // confirm that doc1 and doc3 are sent with their overriden IDs, while doc2 is sent with its actual ID
    assertEquals("doc1_overriden", getCapturedID(captor, 0, 0));
    assertEquals(doc2.getId(), getCapturedID(captor, 1, 0));
    assertEquals("doc3_overriden", getCapturedID(captor, 2, 0));

    assertEquals(3, messenger.getSentEvents().size());

    // confirm that events are sent using original doc IDs, not overriden ones
    List<Event> events = messenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }


  /**
   * Tests that id would be deleted if stated in ignoreFields
   *
   * @throws Exception
   */
  @Test
  public void testIgnoreIdInIgnoreFields() throws Exception {
    Config config = ConfigFactory.load("SolrIndexerTest/ignoreId.conf");
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    doc.setField("myid", "my_new_id");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc); // seems to capture the document ID here before the indexing portion
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    assertEquals(1, messenger.getSentEvents().size());

    // confirm that the document id has been removed
    SolrInputDocument solrDoc = (SolrInputDocument) captor.getAllValues().get(0).toArray()[0];
    assertNull(solrDoc.getFieldValue("id"));
    assertEquals("my_new_id", solrDoc.getFieldValue("myid"));
  }

  /**
   * test that children documents would obey ignoreFields configurations
   */
  @Test
  public void testIgnoreFieldsInChildren() throws Exception {
    Config config = ConfigFactory.load("SolrIndexerTest/ignoreId.conf");
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document child = Document.create("child", "test_run");
    doc.addChild(child);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc); // seems to capture the document ID here before the indexing portion
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    assertEquals(1, messenger.getSentEvents().size());

    // confirm that the document id has been removed
    SolrInputDocument solrDoc = (SolrInputDocument) captor.getAllValues().get(0).toArray()[0];
    SolrInputDocument childDoc = solrDoc.getChildDocuments().get(0);
    assertNull(childDoc.getFieldValue("id"));
  }

  /**
   * Tests that the indexer correctly handles nested child documents.
   *
   * @throws Exception
   */
  @Test
  public void testIndexerWithNestedChildDocs() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.addToField("myListField", "val1");
    doc2.addToField("myListField", "val2");
    Document doc3 = Document.create("doc3", "test_run");
    doc.addChild(doc2);
    doc.addChild(doc3);
    assertTrue(doc.has(Document.CHILDREN_FIELD));

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(1)).add((captor.capture()));
    verify(solrClient, times(1)).close();
    assertEquals(1, captor.getAllValues().size());
    SolrInputDocument solrDoc = (SolrInputDocument) captor.getAllValues().get(0).toArray()[0];
    assertEquals(doc.getId(), solrDoc.getFieldValue(Document.ID_FIELD));
    assertEquals(2, solrDoc.getChildDocuments().size());
    assertEquals(doc2.getId(), solrDoc.getChildDocuments().get(0).getFieldValue(Document.ID_FIELD));
    Collection<Object> myListField =
        solrDoc.getChildDocuments().get(0).getFieldValues("myListField");
    assertEquals(2, myListField.size());
    assertEquals("val1", myListField.toArray()[0]);
    assertEquals("val2", myListField.toArray()[1]);
    assertEquals(doc3.getId(), solrDoc.getChildDocuments().get(1).getFieldValue(Document.ID_FIELD));
    // the solr doc should not have Document.CHILDREN_FIELD;
    // the children should have been added via solrDoc.addChildDocument
    assertFalse(solrDoc.containsKey(Document.CHILDREN_FIELD));

    assertEquals(1, messenger.getSentEvents().size());
    List<Event> events = messenger.getSentEvents();
    assertEquals(1, events.size());
    assertEquals("doc1", events.get(0).getDocumentId());
    assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  @Test
  public void testSolrIndexerWithMultipleCollections() throws Exception {
    String indexOverrideField = "collection";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1))
            .withValue(
                "indexer.indexOverrideField", ConfigValueFactory.fromAnyRef(indexOverrideField));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField(indexOverrideField, "col1");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.addToField(indexOverrideField, "col2");
    Document doc3 = Document.create("doc3", "test_run");
    doc3.addToField(indexOverrideField, "col1");
    Document doc4 = Document.create("doc4", "test_run");
    doc4.addToField(indexOverrideField, "col2");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    messenger.sendForIndexing(doc4);
    indexer.run(4);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    ArgumentCaptor<String> colCaptor = ArgumentCaptor.forClass(String.class);

    verify(solrClient, times(4)).add(colCaptor.capture(), captor.capture());
    verify(solrClient, times(1)).close();
    assertEquals(4, captor.getAllValues().size());

    // confirm that docs are sent to the correct collection
    Map<String, List<SolrInputDocument>> collectionDocs = new HashMap<>();

    List<Event> events = messenger.getSentEvents();
    for (int i = 0; i < 4; i++) {
      SolrInputDocument sDoc = captor.getAllValues().get(i).stream().findFirst().get();
      String collection = colCaptor.getAllValues().get(i);
      if (collectionDocs.containsKey(collection)) {
        collectionDocs.get(collection).add(sDoc);
      } else {
        List<SolrInputDocument> cDocs = new ArrayList<>();
        cDocs.add(sDoc);
        collectionDocs.put(collection, cDocs);
      }
      // events from different batches can arrive out of order but events from the same
      // batch/collection should arrive
      // in the same order.
      assertEquals(events.get(i).getDocumentId(), sDoc.getFieldValue(Document.ID_FIELD));
      assertEquals(Event.Type.FINISH, events.get(0).getType());
    }
    assertEquals(2, collectionDocs.size());
    assertEquals(2, collectionDocs.get("col1").size());
    assertEquals("doc1", collectionDocs.get("col1").get(0).getFieldValue(Document.ID_FIELD));
    assertEquals("doc3", collectionDocs.get("col1").get(1).getFieldValue(Document.ID_FIELD));
    assertFalse(collectionDocs.get("col1").get(0).containsKey(indexOverrideField));
    assertFalse(collectionDocs.get("col1").get(1).containsKey(indexOverrideField));

    assertEquals(2, collectionDocs.get("col2").size());
    assertEquals("doc2", collectionDocs.get("col2").get(0).getFieldValue(Document.ID_FIELD));
    assertEquals("doc4", collectionDocs.get("col2").get(1).getFieldValue(Document.ID_FIELD));
    assertFalse(collectionDocs.get("col2").get(0).containsKey(indexOverrideField));
    assertFalse(collectionDocs.get("col2").get(1).containsKey(indexOverrideField));
  }

  @Test
  public void testSolrIndexerDelete() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField(deletionMarkerField, deletionMarkerFieldValue);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    indexer.run(1);

    ArgumentCaptor<List<String>> delCaptor = ArgumentCaptor.forClass(List.class);
    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).deleteById(delCaptor.capture());
    inOrder.verify(solrClient).close();

    assertEquals(1, delCaptor.getAllValues().size());
  }

  @Test
  public void testSolrIndexerWithDocumentExplicitlyMarkedAsNotDeleted() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(2))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField(deletionMarkerField, "false");
    Document doc2 = Document.create("doc2", "test_run");
    doc2.addToField(deletionMarkerField, "foo");

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    indexer.run(2);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).add(captor.capture());
    inOrder.verify(solrClient).close();

    assertEquals(1, captor.getAllValues().size());
  }

  @Test
  public void testSolrIndexerAddDeleteAdd() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(3))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField("version", 1);
    Document doc2 = Document.create("doc1", "test_run");
    doc2.addToField(deletionMarkerField, deletionMarkerFieldValue);
    Document doc3 = Document.create("doc1", "test_run");
    doc3.addToField("version", 2);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    ArgumentCaptor<List<String>> delCaptor = ArgumentCaptor.forClass(List.class);

    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).add(captor.capture());
    inOrder.verify(solrClient).deleteById(delCaptor.capture());
    inOrder.verify(solrClient).add(captor.capture());
    inOrder.verify(solrClient).close();

    assertEquals(2, captor.getAllValues().size());
    assertEquals(1, delCaptor.getAllValues().size());

    assertEquals(1, captor.getAllValues().get(0).size());
    assertEquals(1, captor.getAllValues().get(1).size());
    assertEquals(
        2, captor.getAllValues().get(1).stream().findFirst().get().getFieldValue("version"));
  }

  @Test
  public void testSolrIndexerUnrelatedAddAddDeleteAdd() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(4))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document otherDoc1 = Document.create("doc2", "test_run");
    otherDoc1.addToField("version", 1);
    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField("version", 1);
    Document doc2 = Document.create("doc1", "test_run");
    doc2.addToField(deletionMarkerField, deletionMarkerFieldValue);
    Document doc3 = Document.create("doc1", "test_run");
    doc3.addToField("version", 2);

    SolrClient solrClient = mock(SolrClient.class);

    // manually capture all of the docs and ids sent through the solr client.
    List<List<SolrInputDocument>> docsSentToSolr = new ArrayList<>();
    List<List<String>> deleteIdsSentToSolr = new ArrayList<>();

    when(solrClient.add(
        argThat(
            (ArgumentMatcher<Collection>)
                t -> {
                  docsSentToSolr.add(new ArrayList<>(t));
                  return true;
                })))
        .thenReturn(mock(UpdateResponse.class));

    when(solrClient.deleteById(
        argThat(
            (ArgumentMatcher<List>)
                t -> {
                  deleteIdsSentToSolr.add(t);
                  return true;
                })))
        .thenReturn(mock(UpdateResponse.class));

    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(otherDoc1);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(4);

    InOrder inOrder = inOrder(solrClient);
    inOrder.verify(solrClient).add(anyCollection());
    inOrder.verify(solrClient).deleteById(anyList());
    inOrder.verify(solrClient).add(anyCollection());
    inOrder.verify(solrClient).close();
    inOrder.verifyNoMoreInteractions();

    assertEquals("Two separate batches of documents should have been sent to solr", 2, docsSentToSolr.size());
    assertEquals("One batch of deletes should have been sent to solr", 1, deleteIdsSentToSolr.size());
    assertEquals("The first batch of documents sent to solr should have contained 2 documents", 2, docsSentToSolr.get(0).size());
    assertEquals("The first document in the first batch of documents sent to solr should be otherDoc1", otherDoc1.getId(),
        docsSentToSolr.get(0).get(0).getFieldValue(Document.ID_FIELD));
    assertEquals("The second document in the first batch of documents sent to solr should be doc1", doc1.getId(),
        docsSentToSolr.get(0).get(1).getFieldValue(Document.ID_FIELD));
    assertEquals("The batch of deletes sent to solr should contain one id to delete", 1, deleteIdsSentToSolr.get(0).size());
    assertEquals("The id in the batch of deletes sent to solr should be the id of doc2", doc2.getId(),
        deleteIdsSentToSolr.get(0).get(0));
    assertEquals("The second batch of documents sent to solr should have contained one document", 1, docsSentToSolr.get(1).size());
    assertEquals("The document in the second batch of documents sent to solr should be doc3", doc3.getId(),
        docsSentToSolr.get(1).get(0).getFieldValue(Document.ID_FIELD));
  }

  @Test
  public void testSolrIndexerDeleteAddUnrelatedDelete() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(3))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField(deletionMarkerField, deletionMarkerFieldValue);
    Document doc2 = Document.create("doc1", "test_run");
    doc2.addToField("version", 1);
    Document doc3 = Document.create("doc2", "test_run");
    doc3.addToField(deletionMarkerField, deletionMarkerFieldValue);

    SolrClient solrClient = mock(SolrClient.class);

    // manually capture all of the docs and ids sent through the solr client.
    List<List<SolrInputDocument>> docsSentToSolr = new ArrayList<>();
    List<List<String>> deleteIdsSentToSolr = new ArrayList<>();

    when(solrClient.add(
        argThat(
            (ArgumentMatcher<Collection>)
                t -> {
                  docsSentToSolr.add(new ArrayList<>(t));
                  return true;
                })))
        .thenReturn(mock(UpdateResponse.class));

    when(solrClient.deleteById(
        argThat(
            (ArgumentMatcher<List>)
                t -> {
                  deleteIdsSentToSolr.add(t);
                  return true;
                })))
        .thenReturn(mock(UpdateResponse.class));

    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).deleteById(anyList());
    inOrder.verify(solrClient).add(anyCollection());
    inOrder.verify(solrClient).deleteById(anyList());
    inOrder.verify(solrClient).close();
    inOrder.verifyNoMoreInteractions();

    assertEquals("There should have been one batch of documents sent to solr", 1, docsSentToSolr.size());
    assertEquals("There should have been 2 batches of ids to delete sent to solr", 2, deleteIdsSentToSolr.size());
    assertEquals("The first batch of ids to delete sent to solr should have contained one id", 1,
        deleteIdsSentToSolr.get(0).size());
    assertEquals("The id in the first batch of ids to delete sent to solr should be the id of doc1", doc1.getId(),
        deleteIdsSentToSolr.get(0).get(0));
    assertEquals("The batch of docs added to solr should contain one document", 1, docsSentToSolr.get(0).size());
    assertEquals("The document in the first batch of documents added to solr should be doc2", doc2.getId(),
        docsSentToSolr.get(0).get(0).getFieldValue(Document.ID_FIELD));
    assertEquals("The second batch of ids to delete sent to solr should have contained one id", 1,
        deleteIdsSentToSolr.get(1).size());
    assertEquals("The id in the second batch of ids to delete sent to solr should be the id of doc3", doc3.getId(),
        deleteIdsSentToSolr.get(1).get(0));
  }

  @Test
  public void testSolrIndexerDeleteAddDelete() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(3))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField(deletionMarkerField, deletionMarkerFieldValue);
    Document doc2 = Document.create("doc1", "test_run");
    doc2.addToField("version", 1);
    Document doc3 = Document.create("doc1", "test_run");
    doc3.addToField(deletionMarkerField, deletionMarkerFieldValue);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    ArgumentCaptor<List<String>> delCaptor = ArgumentCaptor.forClass(List.class);

    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).deleteById(delCaptor.capture());
    inOrder.verify(solrClient).add(captor.capture());
    inOrder.verify(solrClient).deleteById(delCaptor.capture());
    inOrder.verify(solrClient).close();

    assertEquals(1, captor.getAllValues().size());
    assertEquals(2, delCaptor.getAllValues().size());
  }


  @Test
  public void testSolrIndexerAddAddDelete() throws Exception {
    String deletionMarkerField = "is_deleted";
    String deletionMarkerFieldValue = "true";

    Config config =
        ConfigFactory.empty()
            .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(3))
            .withValue(
                "indexer.deletionMarkerField", ConfigValueFactory.fromAnyRef(deletionMarkerField))
            .withValue(
                "indexer.deletionMarkerFieldValue",
                ConfigValueFactory.fromAnyRef(deletionMarkerFieldValue));
    TestMessenger messenger = new TestMessenger();

    Document doc1 = Document.create("doc1", "test_run");
    doc1.addToField("version", 1);
    Document doc2 = Document.create("doc1", "test_run");
    doc2.addToField("version", 2);
    Document doc3 = Document.create("doc1", "test_run");
    doc3.addToField(deletionMarkerField, deletionMarkerFieldValue);

    SolrClient solrClient = mock(SolrClient.class);

    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc1);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    indexer.run(3);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    ArgumentCaptor<List<String>> delCaptor = ArgumentCaptor.forClass(List.class);

    InOrder inOrder = inOrder(solrClient);

    inOrder.verify(solrClient).add(captor.capture());
    inOrder.verify(solrClient).deleteById(delCaptor.capture());
    inOrder.verify(solrClient).close();
  }

  // test for when the method throws an exception, all the documents fail
  @Test
  public void testSolrException() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("SolrIndexerTest/exception.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    Indexer indexer = new ErroringIndexer(config, messenger, true);
    messenger.sendForIndexing(doc);
    messenger.sendForIndexing(doc2);
    messenger.sendForIndexing(doc3);
    messenger.sendForIndexing(doc4);
    messenger.sendForIndexing(doc5);
    indexer.run(5);

    List<Event> events = messenger.getSentEvents();
    assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  // test for when specific documents fail, no exception is thrown.
  @Test
  public void testSolrDocumentErrors() throws Exception {
    Config config = ConfigFactory.empty()
        .withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1))
        .withValue("indexer.indexOverrideField", ConfigValueFactory.fromAnyRef("other_collection"))
        .withValue("indexer.idOverrideField", ConfigValueFactory.fromAnyRef("other_id"));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");

    // making sure that we are still getting the correct ids for idOverride.
    Document doc2 = Document.create("doc2", "test_run");
    doc2.setField("other_collection", "collection1");
    doc2.setField("other_id", "idForDoc2");

    Document doc3 = Document.create("doc3", "test_run");
    doc3.setField("other_collection", "collection2");
    doc3.setField("other_id", "idForDoc3");

    SolrClient solrClient = mock(SolrClient.class);
    // causes doc to fail (it'll be called w/ no collection specified.)
    when(solrClient.add(any(Collection.class))).thenThrow(new IOException("mock IO Exc"));

    // throw an exception once but fail the other time
    when(solrClient.add(anyString(), any(Collection.class)))
        .thenThrow(new IOException("mock IO Exc"))
        .thenReturn(null);

    SolrIndexer indexer = new SolrIndexer(config, messenger, "", solrClient);

    Set<Pair<Document, String>> failedDocs = indexer.sendToIndex(List.of(doc, doc2, doc3));
    assertEquals(2, failedDocs.size());
    assertTrue(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc)));

    // not sure what is, internally to solr, controlling the ordering behind which document is sent first/not.
    // so, will just check that both of them do not have the same status (failed/succeeded)
    assertNotEquals(failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc2)),
        failedDocs.stream().anyMatch(p -> p.getLeft().equals(doc3)));
  }

  @Test
  public void testIndexerWithNestedJson() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\":1, \"b\":2}");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithChildDocWithNestedJson() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document childDoc = Document.create("child1");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\":1, \"b\":2}");
    childDoc.setField("myJsonField", jsonNode);
    doc.addChild(childDoc);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithNestedJsonMultivalued() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithChildDocWithNestedJsonMultivalued() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document childDoc = Document.create("child1");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": [{\"aa\":1}, {\"aa\": 2}] }");
    childDoc.setField("myJsonField", jsonNode);
    doc.addChild(childDoc);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithNestedJsonWithObjects() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    doc.setField("myJsonField", jsonNode);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test
  public void testIndexerWithChildDocWithNestedJsonWithObjects() throws Exception {
    Config config =
        ConfigFactory.empty().withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1", "test_run");
    Document childDoc = Document.create("child1");
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.readTree("{\"a\": {\"aa\":1}, \"b\":{\"ab\": 2} }");
    childDoc.setField("myJsonField", jsonNode);
    doc.addChild(childDoc);

    SolrClient solrClient = mock(SolrClient.class);
    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);
    verify(solrClient, times(0)).add((captor.capture()));
    List<Event> events = messenger.getSentEvents();
    MatcherAssert.assertThat(1, equalTo(events.size()));
    MatcherAssert.assertThat(
        "Attempting to index a document with a nested object field to solr should result in an "
            + "indexing failure event.",
        Event.Type.FAIL,
        equalTo(events.get(0).getType()));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUseCloudClientConfigException() {
    Config config = ConfigFactory.empty()
        .withValue("indexer.type", ConfigValueFactory.fromAnyRef("solr"))
        .withValue("solr.useCloudClient", ConfigValueFactory.fromAnyRef(true))
        // This should be a list of strings
        .withValue("solr.zkHosts", ConfigValueFactory.fromAnyRef("hello"));
    TestMessenger messenger = new TestMessenger();
    new SolrIndexer(config, messenger, false, "", null);
  }

  /**
   * Tests that the indexer remove the fields in ignoreFields configuration before sending to the Client
   *
   * @throws Exception
   */
  @Test
  public void testIgnoreFieldsConfig() throws Exception {
    Config config = ConfigFactory.load("SolrIndexerTest/ignoreFields.conf");
    TestMessenger messenger = new TestMessenger();

    Document doc = Document.create("doc1");
    doc.setField("ignoreField1", "value1");
    doc.setField("ignoreField2", "value2");
    doc.setField("normalField", "normalValue");

    // check that the document has fields added above
    assertNotNull(doc.getString("ignoreField1"));
    assertNotNull(doc.getString("ignoreField2"));
    assertNotNull(doc.getString("normalField"));

    SolrClient solrClient = mock(SolrClient.class);

    Indexer indexer = new SolrIndexer(config, messenger, "", solrClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    ArgumentCaptor<Collection<SolrInputDocument>> captor = ArgumentCaptor.forClass(Collection.class);

    // verify that the add method was called on the Client side
    verify(solrClient, times(1)).add(captor.capture());

    // assert that capturedDocs is not empty and only has one document
    Collection<SolrInputDocument> capturedDocs = captor.getValue();
    assertEquals(1, capturedDocs.size());

    SolrInputDocument capturedDoc = capturedDocs.iterator().next();

    // check that ignoreField1 and ignoreField2 has been removed
    assertFalse(capturedDoc.containsKey("ignoreField1"));
    assertFalse(capturedDoc.containsKey("ignoreField2"));
    assertTrue(capturedDoc.containsKey("normalField"));
    assertTrue(capturedDoc.containsKey("id"));
  }

  @Test
  public void testAllowInvalidCertAndAuthHttp() {
    String originalCheckPeerName = System.getProperty("solr.ssl.checkPeerName");
    System.clearProperty("solr.ssl.checkPeerName");

    Config config = ConfigFactory.parseResourcesAnySyntax("SolrIndexerTest/acceptInvalidCert.conf");
    TestMessenger messenger = new TestMessenger();

    Indexer indexer = new SolrIndexer(config, messenger, false, "", null);
    // should be set when we acceptInvalidCert w/ username/password in config
    assertEquals("false", System.getProperty("solr.ssl.checkPeerName"));

    indexer.closeConnection();

    if (originalCheckPeerName != null) {
      System.setProperty("solr.ssl.checkPeerName", originalCheckPeerName);
    }
  }

  @Test
  public void testAllowInvalidCertCloud() {
    String originalCheckPeerName = System.getProperty("solr.ssl.checkPeerName");
    System.clearProperty("solr.ssl.checkPeerName");

    Config config = ConfigFactory.parseResourcesAnySyntax("SolrIndexerTest/acceptInvalidCertCloud.conf");
    TestMessenger messenger = new TestMessenger();

    Indexer indexer = new SolrIndexer(config, messenger, false, "", null);
    // should be set when we acceptInvalidCert for an HTTP client
    assertEquals("false", System.getProperty("solr.ssl.checkPeerName"));

    indexer.closeConnection();

    if (originalCheckPeerName != null) {
      System.setProperty("solr.ssl.checkPeerName", originalCheckPeerName);
    }
  }


  private static String getCapturedID(ArgumentCaptor<Collection<SolrInputDocument>> captor, int index, int arrIndex) {
    SolrInputDocument document = (SolrInputDocument) captor.getAllValues().get(index).toArray()[arrIndex];
    return (String) document.getFieldValue(Document.ID_FIELD);
  }

  public static class ErroringIndexer extends SolrIndexer {

    public static final Spec SPEC = SolrIndexer.SPEC;

    public ErroringIndexer(Config config, IndexerMessenger messenger, boolean bypass) {
      super(config, messenger, bypass, "", null);
    }

    @Override
    public Set<Pair<Document, String>> sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to Solr are correctly handled");
    }
  }
}
