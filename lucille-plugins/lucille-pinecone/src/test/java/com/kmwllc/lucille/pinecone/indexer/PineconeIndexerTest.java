package com.kmwllc.lucille.pinecone.indexer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.configs.PineconeConfig;
import io.pinecone.proto.ListItem;
import io.pinecone.proto.ListResponse;
import io.pinecone.proto.Pagination;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.pinecone.proto.VectorServiceGrpc;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;
import org.openapitools.control.client.model.IndexModel;
import org.openapitools.control.client.model.IndexModelStatus;

public class PineconeIndexerTest {

  private VectorServiceGrpc.VectorServiceBlockingStub stub;

  private Document doc0;
  private Document doc1;
  private Document doc2;

  private List<Float> doc0ForNamespace1;
  private List<Float> doc0ForNamespace2;
  private List<Float> doc1ForNamespace1;
  private List<Float> doc1ForNamespace2;

  private IndexModel goodIndexModel;
  private IndexModel shutdownIndexModel;
  private IndexModel failureIndexModel;
  private Index goodIndex;
  private Index failureIndex;
  private Index shutdownIndex;

  @Before
  public void setup() {
    setUpIndexes();
    setUpDocuments();
  }

  private void setUpDocuments() {
    doc0 = Document.create("doc0");
    doc1 = Document.create("doc1");
    doc2 = Document.create("doc2"); // empty doc without embeddings
    doc0ForNamespace1 = List.of(1.0f, 2.0f);
    doc0ForNamespace2 = List.of(3.0f, 4.0f);
    doc1ForNamespace1 = List.of(5.0f, 6.0f);
    doc1ForNamespace2 = List.of(7.0f, 8.0f);

    doc0.update("vector-for-namespace1", UpdateMode.OVERWRITE, doc0ForNamespace1.toArray(new Float[0]));
    doc0.update("vector-for-namespace2", UpdateMode.OVERWRITE, doc0ForNamespace2.toArray(new Float[0]));
    doc0.update("metaString1", UpdateMode.OVERWRITE, "some string data");
    doc0.update("metaString2", UpdateMode.OVERWRITE, "some more string data");
    doc0.update("metaList", UpdateMode.OVERWRITE, 1, 2, 3);
    doc1.update("vector-for-namespace1", UpdateMode.OVERWRITE, doc1ForNamespace1.toArray(new Float[0]));
    doc1.update("vector-for-namespace2", UpdateMode.OVERWRITE, doc1ForNamespace2.toArray(new Float[0]));
    doc1.update("metaString1", UpdateMode.OVERWRITE, "some string data 2");
    doc1.update("metaString2", UpdateMode.OVERWRITE, "some more string data 2");
    doc1.update("metaList", UpdateMode.OVERWRITE, 4, 5, 6);
  }

  private void setUpIndexes() {
    goodIndexModel = Mockito.mock(IndexModel.class);
    goodIndex = Mockito.mock(Index.class);
    IndexModelStatus goodStatus = Mockito.mock(IndexModelStatus.class);
    when(goodIndexModel.getStatus()).thenReturn(goodStatus);
    when(goodStatus.getState()).thenReturn(StateEnum.READY);

    failureIndexModel = Mockito.mock(IndexModel.class);
    failureIndex = Mockito.mock(Index.class);
    IndexModelStatus failureStatus = Mockito.mock(IndexModelStatus.class);
    when(failureIndexModel.getStatus()).thenReturn(failureStatus);
    when(failureStatus.getState()).thenReturn(StateEnum.INITIALIZATIONFAILED);

    shutdownIndexModel = Mockito.mock(IndexModel.class);
    shutdownIndex = Mockito.mock(Index.class);
    IndexModelStatus shutdownStatus = Mockito.mock(IndexModelStatus.class);
    when(shutdownIndexModel.getStatus()).thenReturn(shutdownStatus);
    when(shutdownStatus.getState()).thenReturn(StateEnum.TERMINATING);
  }

  @Test
  public void testClientCreatedWithCorrectConfig() {
    Map<Pinecone, List<Object>> constructorArgs = new HashMap<>();

    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      constructorArgs.put(mock, new ArrayList<>(context.arguments()));
    })) {
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      TestMessenger messenger = new TestMessenger();
      new PineconeIndexer(configGood, messenger, "testing");

      assertTrue(client.constructed().size() == 1);
      Pinecone constructed = client.constructed().get(0);
      assertTrue(constructorArgs.get(constructed).get(0) instanceof PineconeConfig);

      PineconeConfig config = (PineconeConfig) constructorArgs.get(constructed).get(0);

      assertEquals("apiKey", config.getApiKey());
    }
  }

  @Test
  public void testValidateConnection() {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
      when(mock.describeIndex("failure")).thenReturn(failureIndexModel);
      when(mock.describeIndex("shutdown")).thenReturn(shutdownIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      Config configFailure = ConfigFactory.load("PineconeIndexerTest/failure-config.conf");
      Config configShutdown = ConfigFactory.load("PineconeIndexerTest/shutdown-config.conf");

      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      PineconeIndexer indexerFailure = new PineconeIndexer(configFailure, messenger, "testing");
      PineconeIndexer indexerShutdown = new PineconeIndexer(configShutdown, messenger, "testing");

      assertTrue(indexerGood.validateConnection());
      assertFalse(indexerFailure.validateConnection());
      assertFalse(indexerShutdown.validateConnection());
    }
  }

  @Test
  public void testCloseConnection() {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");

      assertTrue(indexerGood.validateConnection());
      indexerGood.closeConnection();
      verify(goodIndex, times(1)).close();
    }
  }

  @Test
  public void testUpsertAndUpdateEmptyNamespacesProvided() {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
    })) {
      TestMessenger messenger = new TestMessenger();
      TestMessenger messenger2 = new TestMessenger();
      Config configUpsert = ConfigFactory.load("PineconeIndexerTest/empty-namespaces.conf");
      Config configUpdate = ConfigFactory.load("PineconeIndexerTest/empty-namespaces-update.conf");

      assertThrows(IllegalArgumentException.class, () -> {
        new PineconeIndexer(configUpdate, messenger, "testing");
      });

      assertThrows(IllegalArgumentException.class, () -> {
        new PineconeIndexer(configUpsert, messenger2, "testing");
      });

    }
  }

  @Test
  public void testUpsertNoNamespacesProvided() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);

      UpsertResponse response = Mockito.mock(UpsertResponse.class);
      when(response.getUpsertedCount()).thenReturn(2);

      when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configUpsert = ConfigFactory.load("PineconeIndexerTest/no-namespaces.conf");

      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpsert, messenger, "testing");

      indexerUpsert.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerUpsert.run(2);

      // assert that no updates have been made
      verify(goodIndex, times(0)).update(Mockito.any(), Mockito.any(), Mockito.any());
      // assert that an upsert was made to the right nameSpace
      ArgumentCaptor<String> nameSpaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(1)).upsert(anyList(), nameSpaceUsed.capture());
      assertEquals("default", nameSpaceUsed.getValue());
    }
  }

  @Test
  public void testUpdateNoNamespacesProvided() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configUpdate = ConfigFactory.load("PineconeIndexerTest/no-namespaces-update.conf");

      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpdate, messenger, "testing");

      indexerUpsert.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerUpsert.run(2);

      // assert that an update was made
      ArgumentCaptor<String> nameSpaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(2)).update(Mockito.any(), Mockito.any(), nameSpaceUsed.capture());
      // assert that no upserts have been made
      verify(goodIndex, times(0)).upsert(Mockito.any(), nameSpaceUsed.capture());

      assertEquals("default", nameSpaceUsed.getAllValues().get(0));
      assertEquals("default", nameSpaceUsed.getAllValues().get(1));
    }
  }

  @Test
  public void testUpsertMultipleNamespaces() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      UpsertResponse response = Mockito.mock(UpsertResponse.class);
      when(response.getUpsertedCount()).thenReturn(2);

      when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      // make sure no updates were made
      verify(goodIndex, times(0)).update(Mockito.anyString(), Mockito.any(), Mockito.anyString());
      // make sure two upserts were made
      ArgumentCaptor<List<VectorWithUnsignedIndices>> vectorCaptor = ArgumentCaptor.forClass(List.class);
      ArgumentCaptor<String> namespaceCaptor = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(2)).upsert(vectorCaptor.capture(), namespaceCaptor.capture());

      List<List<VectorWithUnsignedIndices>> vectors = vectorCaptor.getAllValues();

      List<VectorWithUnsignedIndices> namespace2Upsert = vectors.get(0);
      List<VectorWithUnsignedIndices> namespace1Upsert = vectors.get(1);

      assertEquals("namespace-1", namespaceCaptor.getAllValues().get(1));
      assertEquals("namespace-2", namespaceCaptor.getAllValues().get(0));

      assertEquals(2, namespace1Upsert.size());
      assertEquals(2, namespace2Upsert.size());

      // make sure vectors are correct for each document and namespace
      assertEquals(doc0ForNamespace1, namespace1Upsert.get(0).getValuesList());
      assertEquals(doc0ForNamespace2, namespace2Upsert.get(0).getValuesList());
      assertEquals(doc1ForNamespace1, namespace1Upsert.get(1).getValuesList());
      assertEquals(doc1ForNamespace2, namespace2Upsert.get(1).getValuesList());
    }
  }

  @Test
  public void testCorrectMetadata() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);

      UpsertResponse response = Mockito.mock(UpsertResponse.class);
      when(response.getUpsertedCount()).thenReturn(2);
      when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      ArgumentCaptor<List<VectorWithUnsignedIndices>> captor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(2)).upsert(captor.capture(), Mockito.anyString());
      List<VectorWithUnsignedIndices> namespace1Upsert = captor.getAllValues().get(0);
      List<VectorWithUnsignedIndices> namespace2Upsert = captor.getAllValues().get(1);

      // make sure metadata is correct
      assertEquals(namespace1Upsert.get(0).getMetadata().getFieldsMap().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace1Upsert.get(0).getMetadata().getFieldsMap().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace1Upsert.get(1).getMetadata().getFieldsMap().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace1Upsert.get(1).getMetadata().getFieldsMap().get("metaList").toString(),
          "string_value: \"[4, 5, 6]\"\n");
      assertEquals(namespace2Upsert.get(0).getMetadata().getFieldsMap().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace2Upsert.get(0).getMetadata().getFieldsMap().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace2Upsert.get(1).getMetadata().getFieldsMap().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace2Upsert.get(1).getMetadata().getFieldsMap().get("metaList").toString(),
          "string_value: \"[4, 5, 6]\"\n");

      // make sure there are no additional metadata fields
      assertEquals(2, namespace1Upsert.get(0).getMetadata().getFieldsMap().entrySet().size());
      assertEquals(2, namespace1Upsert.get(1).getMetadata().getFieldsMap().entrySet().size());
      assertEquals(2, namespace2Upsert.get(0).getMetadata().getFieldsMap().entrySet().size());
      assertEquals(2, namespace2Upsert.get(1).getMetadata().getFieldsMap().entrySet().size());
    }
  }


  @Test
  public void testUpdateMultipleNamespaces() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces-update.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      // make sure four updates were made (one update per document per namespace)
      ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<List<Float>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(4)).update(anyString(), listArgumentCaptor.capture(), stringArgumentCaptor.capture());
      // make sure no upserts were made
      ArgumentCaptor<String> stringArgumentCaptor2 = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<List<VectorWithUnsignedIndices>> listArgumentCaptor2 = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(0)).upsert(listArgumentCaptor2.capture(), stringArgumentCaptor2.capture());

      String namespace2Request1 = stringArgumentCaptor.getAllValues().get(0);
      String namespace2Request2 = stringArgumentCaptor.getAllValues().get(1);
      String namespace1Request1 = stringArgumentCaptor.getAllValues().get(2);
      String namespace1Request2 = stringArgumentCaptor.getAllValues().get(3);

      assertEquals("namespace-1", namespace1Request1);
      assertEquals("namespace-1", namespace1Request2);
      assertEquals("namespace-2", namespace2Request1);
      assertEquals("namespace-2", namespace2Request2);

      // make sure vectors are correct for each document and namespace
      List<List<Float>> values = listArgumentCaptor.getAllValues();
      assertEquals(doc0ForNamespace2, values.get(0));
      assertEquals(doc1ForNamespace2, values.get(1));
      assertEquals(doc0ForNamespace1, values.get(2));
      assertEquals(doc1ForNamespace1, values.get(3));

      // No metadata is provided when doing updates so testing is unecessary
    }
  }

  @Test
  public void testDeletionById() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/deletion-config.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      // make sure 2 deletion were made (one deletion per document)
      ArgumentCaptor<List<String>> ListArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(ListArgumentCaptor.capture(), anyString());

      // make sure vectors are correct for each document and namespace
      List<String> idsSentForDeletion = ListArgumentCaptor.getAllValues().get(0);
      assertEquals(doc0.getId(), idsSentForDeletion.get(0));
      assertEquals(doc1.getId(), idsSentForDeletion.get(1));
    }
  }

  @Test
  public void testDeletionWithAddPrefix() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/deletion-config-with-prefix.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      // mocking
      String prefix = "-";
      ListItem item1 = ListItem.newBuilder().setId(doc0.getId()+"-1").build();
      ListItem item2 = ListItem.newBuilder().setId(doc0.getId()+"-2").build();
      ListItem item3 = ListItem.newBuilder().setId(doc0.getId()+"-3").build();
      List<ListItem> vectorList = Arrays.asList(item1, item2, item3);

      ListResponse response = mock(ListResponse.class);
      when(response.getVectorsList()).thenReturn(vectorList);
      when(response.hasPagination()).thenReturn(false);
      when(goodIndex.list(anyString(), anyString())).thenReturn(response);

      messenger.sendForIndexing(doc0);
      indexerGood.run(1);

      // make sure 1 deletion were made
      ArgumentCaptor<List<String>> ListArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(ListArgumentCaptor.capture(), anyString());
      ArgumentCaptor<String> stringArgumentCaptor = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(1)).list(anyString(), stringArgumentCaptor.capture());

      // make sure list is called with the right prefix
      assertEquals(doc0.getId()+prefix, stringArgumentCaptor.getValue());
      // make sure deletion ids have included prefix
      List<String> idsSentForDeletion = ListArgumentCaptor.getAllValues().get(0);
      assertEquals(doc0.getId()+"-1", idsSentForDeletion.get(0));
      assertEquals(doc0.getId()+"-2", idsSentForDeletion.get(1));
      assertEquals(doc0.getId()+"-3", idsSentForDeletion.get(2));
    }
  }


  @Test
  public void testDeletionWithAddPrefixWithPagination() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/deletion-config-with-prefix.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      // mocking
      ListItem item1 = ListItem.newBuilder().setId(doc0.getId()+"-1").build();
      ListItem item2 = ListItem.newBuilder().setId(doc0.getId()+"-2").build();
      List<ListItem> vectorList = Arrays.asList(item1, item2);
      Pagination pagination = Mockito.mock(Pagination.class);
      when(pagination.getNext()).thenReturn("token");

      ListResponse response = mock(ListResponse.class);
      when(response.getVectorsList()).thenReturn(vectorList);
      when(response.hasPagination()).thenReturn(true);
      when (response.getPagination()).thenReturn(pagination);

      ListItem item3 = ListItem.newBuilder().setId(doc0.getId()+"-3").build();
      List<ListItem> vectorList2 = List.of(item3);
      ListResponse response2 = mock(ListResponse.class);
      when(response2.getVectorsList()).thenReturn(vectorList2);
      when(response2.hasPagination()).thenReturn(false);

      when(goodIndex.list(anyString(), anyString())).thenReturn(response);
      when(goodIndex.list(anyString(), anyString(), anyString())).thenReturn(response2);

      messenger.sendForIndexing(doc0);
      indexerGood.run(1);

      // make sure 1 deletion were made
      ArgumentCaptor<List<String>> ListArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(ListArgumentCaptor.capture(), anyString());

      // make sure deletion ids have included prefix
      List<String> idsSentForDeletion = ListArgumentCaptor.getAllValues().get(0);
      assertEquals(doc0.getId()+"-1", idsSentForDeletion.get(0));
      assertEquals(doc0.getId()+"-2", idsSentForDeletion.get(1));
      assertEquals(doc0.getId()+"-3", idsSentForDeletion.get(2));
    }
  }
}
