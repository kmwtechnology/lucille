package com.kmwllc.lucille.pinecone.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.configs.PineconeConfig;
import io.pinecone.unsigned_indices_model.VectorWithUnsignedIndices;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import io.pinecone.proto.UpdateRequest;
import io.pinecone.proto.UpsertRequest;
import io.pinecone.proto.VectorServiceGrpc;
import org.openapitools.client.model.IndexModel;
import org.openapitools.client.model.IndexModelStatus;
import org.openapitools.client.model.IndexModelStatus.StateEnum;

public class PineconeIndexerTest {

  private VectorServiceGrpc.VectorServiceBlockingStub stub;

  private Document doc0;
  private Document doc1;

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
    Mockito.when(goodIndexModel.getStatus()).thenReturn(goodStatus);
    Mockito.when(goodStatus.getState()).thenReturn(StateEnum.READY);

    failureIndexModel = Mockito.mock(IndexModel.class);
    failureIndex = Mockito.mock(Index.class);
    IndexModelStatus failureStatus = Mockito.mock(IndexModelStatus.class);
    Mockito.when(failureIndexModel.getStatus()).thenReturn(failureStatus);
    Mockito.when(failureStatus.getState()).thenReturn(StateEnum.INITIALIZATIONFAILED);

    shutdownIndexModel = Mockito.mock(IndexModel.class);
    shutdownIndex = Mockito.mock(Index.class);
    IndexModelStatus shutdownStatus = Mockito.mock(IndexModelStatus.class);
    Mockito.when(shutdownIndexModel.getStatus()).thenReturn(shutdownStatus);
    Mockito.when(shutdownStatus.getState()).thenReturn(StateEnum.TERMINATING);
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
      Mockito.when(mock.describeIndex("good")).thenReturn(goodIndexModel);
      Mockito.when(mock.describeIndex("failure")).thenReturn(failureIndexModel);
      Mockito.when(mock.describeIndex("shutdown")).thenReturn(shutdownIndexModel);
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
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");

      assertTrue(indexerGood.validateConnection());
      indexerGood.closeConnection();
      Mockito.verify(goodIndex, Mockito.times(1)).close();
    }
  }

  @Test
  public void testUpsertAndUpdateEmptyNamespacesProvided() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
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
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configUpsert = ConfigFactory.load("PineconeIndexerTest/no-namespaces.conf");

      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpsert, messenger, "testing");

      indexerUpsert.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerUpsert.run(2);

      // assert that no updates have been made
      Mockito.verify(stub, Mockito.times(0)).update(Mockito.any());
      // assert that an upsert was made
      ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
      Mockito.verify(stub, Mockito.times(1)).upsert(upsertRequest.capture());

      assertEquals("", upsertRequest.getAllValues().get(0).getNamespace());
    }
  }

  @Test
  public void testUpdateNoNamespacesProvided() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configUpdate = ConfigFactory.load("PineconeIndexerTest/no-namespaces-update.conf");

      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpdate, messenger, "testing");

      indexerUpsert.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerUpsert.run(2);

      // assert that an update was made
      ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
      Mockito.verify(stub, Mockito.times(2)).update(updateRequest.capture());
      // assert that no upserts have been made
      Mockito.verify(stub, Mockito.times(0)).upsert(Mockito.any());

      assertEquals("", updateRequest.getAllValues().get(0).getNamespace());
      assertEquals("", updateRequest.getAllValues().get(1).getNamespace());
    }
  }

  @Test
  public void testUpsertMultipleNamespaces() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      Mockito.when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      // make sure no updates were made
      Mockito.verify(goodIndex, Mockito.times(0)).update(Mockito.anyString(), Mockito.any(), Mockito.anyString());
      // make sure two upserts were made
      ArgumentCaptor<List<VectorWithUnsignedIndices>> vectorCaptor = ArgumentCaptor.forClass(List.class);
      ArgumentCaptor<String> namespaceCaptor = ArgumentCaptor.forClass(String.class);
      Mockito.verify(goodIndex, Mockito.times(2)).upsert(vectorCaptor.capture(), namespaceCaptor.capture());

      List<VectorWithUnsignedIndices> namespace2Upsert = vectorCaptor.getAllValues().get(0);
      List<VectorWithUnsignedIndices> namespace1Upsert = vectorCaptor.getAllValues().get(2);

      assertEquals("namespace-1", namespaceCaptor.getAllValues().get(1));
      assertEquals("namespace-2", namespaceCaptor.getAllValues().get(0));

      assertEquals(2, namespace1Upsert.size());
      assertEquals(2, namespace2Upsert.size());

      // make sure vectors are correct for each document and namespace
      assertEquals(doc0ForNamespace1, vectorCaptor.getAllValues().get(0));
      assertEquals(doc1ForNamespace1, vectorCaptor.getAllValues().get(1));
      assertEquals(doc0ForNamespace2, vectorCaptor.getAllValues().get(2));
      assertEquals(doc1ForNamespace2, vectorCaptor.getAllValues().get(3));
    }
  }

  @Test
  public void testCorrectMetadata() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
      Mockito.when(mock.describeIndex("good")).thenReturn(goodIndexModel);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      ArgumentCaptor<List<VectorWithUnsignedIndices>> captor = ArgumentCaptor.forClass(List.class);
      Mockito.verify(goodIndex, Mockito.times(2)).upsert(captor.capture(), Mockito.anyString());
      List<VectorWithUnsignedIndices> namespace1Upsert = captor.getAllValues().get(0);
      List<VectorWithUnsignedIndices> namespace2Upsert = captor.getAllValues().get(1);

      // make sure metadata is correct
      assertEquals(namespace1Upsert.get(0).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace1Upsert.get(0).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace1Upsert.get(1).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace1Upsert.get(1).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[4, 5, 6]\"\n");
      assertEquals(namespace2Upsert.get(0).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace2Upsert.get(0).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace2Upsert.get(1).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace2Upsert.get(1).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[4, 5, 6]\"\n");

      // make sure there are no additional metadata fields
      assertEquals(2, namespace1Upsert.get(0).getMetadata().getFields().entrySet().size());
      assertEquals(2, namespace1Upsert.get(1).getMetadata().getFields().entrySet().size());
      assertEquals(2, namespace2Upsert.get(0).getMetadata().getFields().entrySet().size());
      assertEquals(2, namespace2Upsert.get(1).getMetadata().getFields().entrySet().size());
    }
  }


  @Test
  public void testUpdateMultipleNamespaces() throws Exception {
    try (MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class, (mock, context) -> {
      Mockito.when(mock.getIndexConnection("good")).thenReturn(goodIndex);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces-update.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      // make sure four updates were made (one update per document per namespace)
      ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
      Mockito.verify(stub, Mockito.times(4)).update(updateRequest.capture());
      // make sure no upserts were made
      ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
      Mockito.verify(stub, Mockito.times(0)).upsert(upsertRequest.capture());

      UpdateRequest namespace2Request1 = updateRequest.getAllValues().get(0);
      UpdateRequest namespace2Request2 = updateRequest.getAllValues().get(1);
      UpdateRequest namespace1Request1 = updateRequest.getAllValues().get(2);
      UpdateRequest namespace1Request2 = updateRequest.getAllValues().get(3);

      assertEquals("namespace-1", namespace1Request1.getNamespace());
      assertEquals("namespace-1", namespace1Request2.getNamespace());
      assertEquals("namespace-2", namespace2Request1.getNamespace());
      assertEquals("namespace-2", namespace2Request2.getNamespace());

      // make sure vectors are correct for each document and namespace
      assertEquals(doc0ForNamespace1, namespace1Request1.getValuesList());
      assertEquals(doc1ForNamespace1, namespace1Request2.getValuesList());
      assertEquals(doc0ForNamespace2, namespace2Request1.getValuesList());
      assertEquals(doc1ForNamespace2, namespace2Request2.getValuesList());

      // No metadata is provided when doing updates so testing is unecessary
    }
  }
}
