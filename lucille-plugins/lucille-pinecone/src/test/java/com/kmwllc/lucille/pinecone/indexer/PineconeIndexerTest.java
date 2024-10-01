package com.kmwllc.lucille.pinecone.indexer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.pinecone.util.PineconeManager;
import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.proto.UpsertResponse;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
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
  private Document doc3;
  private Document doc3ToDelete;
  private Document doc4ToDelete;

  private List<Float> doc0ForNamespace1;
  private List<Float> doc0ForNamespace2;
  private List<Float> doc1ForNamespace1;
  private List<Float> doc1ForNamespace2;
  private List<Float> doc3ForNameSpace1;

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
    doc3 = Document.create("doc3");
    doc3ToDelete = Document.create("doc3");
    doc4ToDelete = Document.create("doc4");
    doc0ForNamespace1 = List.of(1.0f, 2.0f);
    doc0ForNamespace2 = List.of(3.0f, 4.0f);
    doc1ForNamespace1 = List.of(5.0f, 6.0f);
    doc1ForNamespace2 = List.of(7.0f, 8.0f);
    doc3ForNameSpace1 = List.of(9.0f, 10.0f);

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
    doc3.update("vector-for-namespace1", UpdateMode.OVERWRITE, doc3ForNameSpace1.toArray(new Float[0]));
    doc3ToDelete.setField("is_deleted", "true");
    doc4ToDelete.setField("is_deleted", "true");
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
  public void testValidateConnection() {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.INITIALIZING).thenReturn(StateEnum.INITIALIZATIONFAILED).thenReturn(StateEnum.TERMINATING).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      PineconeIndexer indexer = new PineconeIndexer(configGood, messenger, "testing");

      // initializing, initializationfailed, terminating, ready
      assertFalse(indexer.validateConnection());
      assertFalse(indexer.validateConnection());
      assertFalse(indexer.validateConnection());
      assertTrue(indexer.validateConnection());
    }
  }

  @Test
  public void testCloseConnection() {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.TERMINATING); // testing for when index should be terminated

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      PineconeIndexer indexer = new PineconeIndexer(configGood, messenger, "testing");
      indexer.closeConnection();
      assertFalse(indexer.validateConnection());
      // should have called close once
      verify(goodIndex, times(1)).close();
    }
  }

  @Test
  public void testUpsertAndUpdateEmptyNamespacesProvided() {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

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
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse response = Mockito.mock(UpsertResponse.class);
    when(response.getUpsertedCount()).thenReturn(2);
    when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

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
      // test that "default" namespace is used when no namespace provided
      assertEquals("default", nameSpaceUsed.getValue());
    }
  }

  @Test
  public void testUpdateNoNamespacesProvided() throws Exception {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configUpdate = ConfigFactory.load("PineconeIndexerTest/no-namespaces-update.conf");
      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpdate, messenger, "testing");

      indexerUpsert.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerUpsert.run(2);

      // assert that update was called twice (called for each document)
      ArgumentCaptor<String> nameSpaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(2)).update(Mockito.any(), Mockito.any(), nameSpaceUsed.capture());
      // assert that no upserts have been made
      verify(goodIndex, times(0)).upsert(Mockito.any(), nameSpaceUsed.capture());
      // test that both were called to "default" when no namespace was provided
      assertEquals("default", nameSpaceUsed.getAllValues().get(0));
      assertEquals("default", nameSpaceUsed.getAllValues().get(1));
    }
  }

  @Test
  public void testUpsertMultipleNamespaces() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse response = Mockito.mock(UpsertResponse.class);
    when(response.getUpsertedCount()).thenReturn(2);
    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

      when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);
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

      // test that the appropriate namespaces were used and the size expected
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
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse response = Mockito.mock(UpsertResponse.class);
    when(response.getUpsertedCount()).thenReturn(2);
    when(goodIndex.upsert(anyList(), anyString())).thenReturn(response);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

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
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
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

      // make sure appropriate namespace was used
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
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/deletion-config.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc3ToDelete);
      messenger.sendForIndexing(doc4ToDelete);
      indexerGood.run(2);

      // make sure a single deletion was called containing all the documents
      ArgumentCaptor<List<String>> ListArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(ListArgumentCaptor.capture(), anyString());

      // make sure vectors are correct for each document and namespace
      List<String> idsSentForDeletion = ListArgumentCaptor.getAllValues().get(0);
      assertEquals(2, idsSentForDeletion.size());
      assertEquals(doc3ToDelete.getId(), idsSentForDeletion.get(0));
      assertEquals(doc4ToDelete.getId(), idsSentForDeletion.get(1));
    }
  }

  @Test
  public void testUpsertAndDeletes() throws Exception {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse mockResponse = mock(UpsertResponse.class);
    when(mockResponse.getUpsertedCount()).thenReturn(2);
    when(goodIndex.upsert(anyList(), anyString())).thenReturn(mockResponse);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/upsert-and-delete.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      messenger.sendForIndexing(doc3ToDelete);
      messenger.sendForIndexing(doc4ToDelete);
      indexerGood.run(4);

      // assert that no updates have been made
      verify(goodIndex, times(0)).update(Mockito.any(), Mockito.any(), Mockito.any());
      // assert that an upsert was made to the right documents to the right nameSpace
      ArgumentCaptor<String> nameSpaceUsed = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<List<VectorWithUnsignedIndices>> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).upsert(listArgumentCaptor.capture(), nameSpaceUsed.capture());
      assertEquals("default", nameSpaceUsed.getValue());
      List<VectorWithUnsignedIndices> vectorIndices = listArgumentCaptor.getValue();
      assertEquals(vectorIndices.size(), 2);
      assertEquals(doc0.getId(), vectorIndices.get(0).getId());
      assertEquals(doc1.getId(), vectorIndices.get(1).getId());

      // make sure a deletion were made (for doc3 and doc4)
      ArgumentCaptor<List<String>> listArgumentCaptor2 = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(listArgumentCaptor2.capture(), anyString());

      // make sure vectors are correct for each document and namespace
      List<String> idsSentForDeletion = listArgumentCaptor2.getAllValues().get(0);
      assertEquals(doc3ToDelete.getId(), idsSentForDeletion.get(0));
      assertEquals(doc4ToDelete.getId(), idsSentForDeletion.get(1));
    }
  }


  @Test
  public void testUpdateAndDeletes() throws Exception {
    // mocking
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse mockResponse = mock(UpsertResponse.class);
    when(mockResponse.getUpsertedCount()).thenReturn(2);
    when(goodIndex.upsert(anyList(), anyString())).thenReturn(mockResponse);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/update-and-delete.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      messenger.sendForIndexing(doc3ToDelete);
      messenger.sendForIndexing(doc4ToDelete);
      indexerGood.run(4);

      // assert that no upserts have been made
      verify(goodIndex, times(0)).upsert(Mockito.any(), Mockito.any(), Mockito.any());
      // assert that an update was made to the right documents to the right Ids
      ArgumentCaptor<String> idsCapture = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<String> namespaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(2)).update(idsCapture.capture(), anyList(), namespaceUsed.capture());
      assertEquals(doc0.getId(), idsCapture.getAllValues().get(0));
      assertEquals(doc1.getId(), idsCapture.getAllValues().get(1));

      // make sure a deletion were made (for doc3 and doc4)
      ArgumentCaptor<List<String>> listArgumentCaptor2 = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(listArgumentCaptor2.capture(), anyString());

      // make sure vectors are correct for each document and namespace
      List<String> idsSentForDeletion = listArgumentCaptor2.getAllValues().get(0);
      assertEquals(doc3ToDelete.getId(), idsSentForDeletion.get(0));
      assertEquals(doc4ToDelete.getId(), idsSentForDeletion.get(1));
    }
  }


  @Test
  public void testUploadThenDeleteInSameBatch() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);
    UpsertResponse mockResponse = mock(UpsertResponse.class);
    when(mockResponse.getUpsertedCount()).thenReturn(2);
    when(goodIndex.upsert(anyList(), anyString())).thenReturn(mockResponse);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/update-and-delete.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      messenger.sendForIndexing(doc3);
      messenger.sendForIndexing(doc3ToDelete);
      indexerGood.run(4);

      // assert that no upserts have been made
      verify(goodIndex, times(0)).upsert(Mockito.any(), Mockito.any(), Mockito.any());
      // assert that an update was made to the right documents to the right Ids
      ArgumentCaptor<String> idsCapture = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<String> namespaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(2)).update(idsCapture.capture(), anyList(), namespaceUsed.capture());
      assertEquals(2, idsCapture.getAllValues().size()); // doc3 is not added
      assertEquals(doc0.getId(), idsCapture.getAllValues().get(0));
      assertEquals(doc1.getId(), idsCapture.getAllValues().get(1));
      assertEquals("default", namespaceUsed.getValue());

      // make sure a deletion were made (for doc3ToDelete)
      ArgumentCaptor<List<String>> listArgumentCaptor2 = ArgumentCaptor.forClass(List.class);
      verify(goodIndex, times(1)).deleteByIds(listArgumentCaptor2.capture(), anyString());

      // make sure vectors are correct for each document and namespace
      List<String> idsSentForDeletion = listArgumentCaptor2.getAllValues().get(0);
      assertEquals(doc3ToDelete.getId(), idsSentForDeletion.get(0));
    }
  }

  @Test
  public void testDeleteThenUploadInSameBatch() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);
      mockedStatic.when(() -> PineconeManager.getDefaultNamespace()).thenReturn("default");

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/update-and-delete.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc3ToDelete);
      messenger.sendForIndexing(doc3);
      indexerGood.run(2);

      // doc3ToDelete would be deleted from deletedList, and so will not be called
      verify(goodIndex, times(0)).deleteByIds(anyList(), anyString());

      // assert that an update was made to the right documents to the right Ids
      ArgumentCaptor<String> idsCapture = ArgumentCaptor.forClass(String.class);
      ArgumentCaptor<String> namespaceUsed = ArgumentCaptor.forClass(String.class);
      verify(goodIndex, times(1)).update(idsCapture.capture(), anyList(), namespaceUsed.capture());
      assertEquals(doc3.getId(), idsCapture.getAllValues().get(0));
      assertEquals("default", namespaceUsed.getValue());



    }
  }

  @Test
  public void testInvalidBatchSize() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel mockIndexModel = Mockito.mock(IndexModel.class);
    IndexModelStatus mockStatus = Mockito.mock(IndexModelStatus.class);
    when(mockClient.getIndexConnection(anyString())).thenReturn(goodIndex);
    when(mockClient.describeIndex(anyString())).thenReturn(mockIndexModel);
    when(mockIndexModel.getStatus()).thenReturn(mockStatus);
    when(mockStatus.getState()).thenReturn(StateEnum.READY);

    try (MockedStatic<PineconeManager> mockedStatic = Mockito.mockStatic(PineconeManager.class)) {
      mockedStatic.when(() -> PineconeManager.getClient(anyString()))
          .thenReturn(mockClient);

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/invalidBatchSize.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      for (int i = 0; i < 1001; i++) {
        messenger.sendForIndexing(Document.create("id-" + i));
      }
      // will throw error for too many documents sent in batch
      indexerGood.run(1001);
    }
  }
}
