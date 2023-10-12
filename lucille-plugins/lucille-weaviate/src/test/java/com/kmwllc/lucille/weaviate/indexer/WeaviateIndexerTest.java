package com.kmwllc.lucille.weaviate.indexer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.weaviate.client.WeaviateClient;
import io.weaviate.client.base.Response;
import io.weaviate.client.base.Result;
import io.weaviate.client.v1.batch.model.ObjectGetResponse;
import io.weaviate.client.v1.misc.model.Meta;
import java.util.List;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;


public class WeaviateIndexerTest {

  private WeaviateClient mockClient;

  @Before
  public void setup() {
    setupWeaviateClient();
  }

  private void setupWeaviateClient() {
    mockClient = Mockito.mock(WeaviateClient.class);

    Mockito.when(mockClient.misc().metaGetter().run()).thenReturn(
        new Result<>(new Response<>(200, new Meta(), null)),
        new Result<>(new Response<>(404, new Meta(), null))
    );

    Result<ObjectGetResponse[]> mockResponse = Mockito.mock(Result.class);
    Mockito.when(mockClient.batch().objectsBatcher().run()).thenReturn(mockResponse);
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

  /*
    private static class CorruptedWeaviateIndexer extends WeaviateIndexer {

    public CorruptedWeaviateIndexer(Config config, IndexerMessageManager manager,
        RestHighLevelClient client, String metricsPrefix) {
      super(config, manager, client, "testing");
    }

    @Override
    public void sendToIndex(List<Document> docs) throws Exception {
      throw new Exception("Test that errors when sending to indexer are correctly handled");
    }
  }
   */
}