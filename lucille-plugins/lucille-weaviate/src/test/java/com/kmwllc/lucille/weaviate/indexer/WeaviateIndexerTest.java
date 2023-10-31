package com.kmwllc.lucille.weaviate.indexer;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Result;
import io.weaviate.client.base.WeaviateError;
import io.weaviate.client.v1.batch.Batch;
import io.weaviate.client.v1.batch.api.ObjectsBatcher;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.data.model.WeaviateObject;
import io.weaviate.client.v1.data.replication.model.ConsistencyLevel;
import io.weaviate.client.v1.misc.Misc;
import io.weaviate.client.v1.misc.api.MetaGetter;
import io.weaviate.client.v1.misc.model.Meta;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;


public class WeaviateIndexerTest {

  private WeaviateClient mockClient;
  private ObjectsBatcher mockObjectsBatcher;

  @Before
  public void setup() {
    setupWeaviateClient();
  }

  private void setupWeaviateClient() {
    mockClient = Mockito.mock(WeaviateClient.class);

    Misc mockMisc = Mockito.mock(Misc.class);
    Mockito.when(mockClient.misc()).thenReturn(mockMisc);

    MetaGetter mockMetaGetter = Mockito.mock(MetaGetter.class);
    Mockito.when(mockMisc.metaGetter()).thenReturn(mockMetaGetter);

    Result<Meta> mockMetaResult = Mockito.mock(Result.class);
    Mockito.when(mockMetaGetter.run()).thenReturn(mockMetaResult);
    // return null first and then 404 error
    Mockito.when(mockMetaResult.getError()).thenReturn(null,
        new WeaviateError(404, new ArrayList<>()));

    Meta mockMeta = Mockito.mock(Meta.class);
    Mockito.when(mockMeta.getHostname()).thenReturn("localhost");
    Mockito.when(mockMeta.getVersion()).thenReturn("1.0.0");
    Mockito.when(mockMeta.getModules()).thenReturn("modules");

    Mockito.when(mockMetaResult.getResult()).thenReturn(mockMeta);

    Batch mockBatch = Mockito.mock(Batch.class);
    Mockito.when(mockClient.batch()).thenReturn(mockBatch);

    mockObjectsBatcher = Mockito.mock(ObjectsBatcher.class);
    Mockito.when(mockBatch.objectsBatcher()).thenReturn(mockObjectsBatcher);
    Mockito.when(mockObjectsBatcher.withConsistencyLevel(ConsistencyLevel.ALL)).thenReturn(mockObjectsBatcher);

    Result<ObjectGetResponse[]> mockResponse = Mockito.mock(Result.class);
    Mockito.when(mockObjectsBatcher.run()).thenReturn(mockResponse);
    Mockito.when(mockResponse.getResult()).thenReturn(new ObjectGetResponse[]{});
  }

  @Test
  public void testWeaviateIndexer() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");

    WeaviateIndexer indexer = new WeaviateIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    indexer.run(2);

    Assert.assertEquals(2, manager.getSavedEvents().size());

    List<Event> events = manager.getSavedEvents();
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  @Test
  public void testWeaviateIndexerException() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    WeaviateIndexer indexer = new CorruptedWeaviateIndexer(config, manager, mockClient, "testing");
    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals(5, events.size());
    for (int i = 1; i <= events.size(); i++) {
      Assert.assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      Assert.assertEquals(Event.Type.FAIL, events.get(i - 1).getType());
    }
  }

  @Test
  public void testValidateConnection() {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("WeaviateIndexerTest/config.conf");
    WeaviateIndexer indexer = new WeaviateIndexer(config, manager, mockClient, "testing");
    Assert.assertTrue(indexer.validateConnection()); // should only work the first time with the mockClient
    Assert.assertFalse(indexer.validateConnection());
    Assert.assertFalse(indexer.validateConnection());
  }

  @Test
  public void testMultipleBatches() throws Exception {
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Config config = ConfigFactory.load("WeaviateIndexerTest/batching.conf");
    WeaviateIndexer indexer = new WeaviateIndexer(config, manager, mockClient, "testing");

    Document doc = Document.create("doc1", "test_run");
    Document doc2 = Document.create("doc2", "test_run");
    Document doc3 = Document.create("doc3", "test_run");
    Document doc4 = Document.create("doc4", "test_run");
    Document doc5 = Document.create("doc5", "test_run");

    manager.sendCompleted(doc);
    manager.sendCompleted(doc2);
    manager.sendCompleted(doc3);
    manager.sendCompleted(doc4);
    manager.sendCompleted(doc5);
    indexer.run(5);

    ArgumentCaptor<WeaviateObject> bulkRequestArgumentCaptor = ArgumentCaptor.forClass(WeaviateObject.class);

    // todo should i be capturing a different method to test that there were multiple batches?
    verify(mockObjectsBatcher, times(5)).withObject(bulkRequestArgumentCaptor.capture());

    List<WeaviateObject> bulkRequestValue = bulkRequestArgumentCaptor.getAllValues();
    assertEquals(5, bulkRequestValue.size());

    WeaviateObject object = bulkRequestArgumentCaptor.getValue();
    assertEquals(WeaviateIndexer.generateDocumentUUID(doc5), object.getId());

// todo is there anything here i can reproduce?
//    BulkRequest br = bulkRequestArgumentCaptor.getValue();
//    List<BulkOperation> requests = br.operations();
//    IndexOperation indexRequest = requests.get(0).index();
//    assertEquals(doc5.getId(), indexRequest.id());


    Assert.assertEquals(5, manager.getSavedEvents().size());
    List<Event> events = manager.getSavedEvents();
    Assert.assertEquals("doc1", events.get(0).getDocumentId());
    Assert.assertEquals(Event.Type.FINISH, events.get(0).getType());
  }

  private static class CorruptedWeaviateIndexer extends WeaviateIndexer {

    public CorruptedWeaviateIndexer(Config config, IndexerMessageManager manager,
        WeaviateClient client, String metricsPrefix) {
      super(config, manager, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
}