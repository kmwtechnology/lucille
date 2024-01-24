package com.kmwllc.lucille.pinecone.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import io.grpc.ConnectivityState;
import io.grpc.ManagedChannel;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.proto.UpdateRequest;
import io.pinecone.proto.UpsertRequest;
import io.pinecone.proto.VectorServiceGrpc;

public class PineconeIndexerTest {

  private VectorServiceGrpc.VectorServiceBlockingStub stub;
  private PineconeConnection goodConnection;
  private PineconeConnection failureConnection;
  private PineconeConnection shutdownConnection;


  private Document doc0;
  private Document doc1;

  private List<Float> doc0ForNamespace1;
  private List<Float> doc0ForNamespace2;
  private List<Float> doc1ForNamespace1;
  private List<Float> doc1ForNamespace2;


  @Before
  public void setup() {
    setUpPineconeClient();
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
    doc1.update("metaList", UpdateMode.OVERWRITE, 1, 2, 3);
  }

  private void setUpPineconeClient() {
    stub = Mockito.mock(VectorServiceGrpc.VectorServiceBlockingStub.class);


    goodConnection = Mockito.mock(PineconeConnection.class);
    ManagedChannel goodChannel = Mockito.mock(ManagedChannel.class);
    Mockito.when(goodConnection.getChannel()).thenReturn(goodChannel);
    Mockito.when(goodChannel.getState(true)).thenReturn(ConnectivityState.READY);
    Mockito.when(goodConnection.getBlockingStub()).thenReturn(stub);

    failureConnection = Mockito.mock(PineconeConnection.class);
    ManagedChannel failureChannel = Mockito.mock(ManagedChannel.class);
    Mockito.when(failureConnection.getChannel()).thenReturn(failureChannel);
    Mockito.when(failureChannel.getState(true)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);

    shutdownConnection = Mockito.mock(PineconeConnection.class);
    ManagedChannel shutdownChannel = Mockito.mock(ManagedChannel.class);
    Mockito.when(shutdownConnection.getChannel()).thenReturn(shutdownChannel);
    Mockito.when(shutdownChannel.getState(true)).thenReturn(ConnectivityState.SHUTDOWN);
  }

  @Test
  public void testClientCreatedWithCorrectConfig() {
    Map<PineconeClient, List<Object>> constructorArgs = new HashMap<>();

    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      constructorArgs.put(mock, new ArrayList<>(context.arguments()));
    })) {
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      TestMessenger messenger = new TestMessenger();
      new PineconeIndexer(configGood, messenger, "testing");

      assertTrue(client.constructed().size() == 1);
      PineconeClient constructed = client.constructed().get(0);
      assertTrue(constructorArgs.get(constructed).get(0) instanceof PineconeClientConfig);

      PineconeClientConfig config = (PineconeClientConfig) constructorArgs.get(constructed).get(0);

      assertEquals("apiKey", config.getApiKey());
      assertEquals("environment", config.getEnvironment());
      assertEquals("projectName", config.getProjectName());
      assertEquals(100, config.getServerSideTimeoutSec());

    }

  }

  @Test
  public void testValidateConnection() {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
      Mockito.when(mock.connect("failure")).thenReturn(failureConnection);
      Mockito.when(mock.connect("shutdown")).thenReturn(shutdownConnection);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      Config configFailure = ConfigFactory.load("PineconeIndexerTest/failure-config.conf");
      Config configShutdown = ConfigFactory.load("PineconeIndexerTest/shutdown-config.conf");

      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      PineconeIndexer indexerFailure = new PineconeIndexer(configFailure, messenger, "testing");
      PineconeIndexer indexerShutdown = new PineconeIndexer(configShutdown, messenger, "testing");

      indexerGood.validateConnection();
      Mockito.verify(client.constructed().get(0), Mockito.times(1)).connect("good");
      indexerGood.validateConnection();
      Mockito.verify(client.constructed().get(0), Mockito.times(1)).connect("good");

      assertTrue(indexerGood.validateConnection());
      assertFalse(indexerFailure.validateConnection());
      assertFalse(indexerShutdown.validateConnection());
    }
  }

  @Test
  public void testCloseConnection() {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {
      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");

      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.closeConnection();
      Mockito.verify(goodConnection, Mockito.times(0)).close();
      indexerGood.validateConnection();
      indexerGood.closeConnection();
      Mockito.verify(goodConnection, Mockito.times(1)).close();
    }
  }


  @Test
  public void testUpsertAndUpdateNoNamespacesProvided() throws Exception {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {

      TestMessenger messenger = new TestMessenger();
      TestMessenger messenger2 = new TestMessenger();
      Config configUpsert = ConfigFactory.load("PineconeIndexerTest/no-namespaces.conf");
      Config configUpdate = ConfigFactory.load("PineconeIndexerTest/no-namespaces-update.conf");

      PineconeIndexer indexerUpdate = new PineconeIndexer(configUpdate, messenger, "testing");
      PineconeIndexer indexerUpsert = new PineconeIndexer(configUpsert, messenger2, "testing");

      indexerUpsert.validateConnection();
      indexerUpdate.validateConnection();


      Document doc0 = Document.create("doc0");
      Document doc1 = Document.create("doc1");
      doc0.update("doc0", UpdateMode.OVERWRITE, 1.0, 2.0);
      doc1.update("doc1", UpdateMode.OVERWRITE, 3.0, 4.0);

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      messenger2.sendForIndexing(doc0);
      messenger2.sendForIndexing(doc1);
      indexerUpdate.run(2);
      indexerUpsert.run(2);

      // assert that no upserts or updates have happened
      Mockito.verify(stub, Mockito.times(0)).update(Mockito.any());
      Mockito.verify(stub, Mockito.times(0)).upsert(Mockito.any());

    }
  }


  @Test
  public void testUpsertMultipleNamespaces() throws Exception {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
      Mockito.verify(stub, Mockito.times(0)).update(Mockito.any());
      ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
      Mockito.verify(stub, Mockito.times(2)).upsert(upsertRequest.capture());

      // make sure no updates were made
      assertTrue(updateRequest.getAllValues().isEmpty());

      // make sure two upserts were made
      assertTrue(upsertRequest.getAllValues().size() == 2);
      UpsertRequest namespace1Upsert = upsertRequest.getAllValues().get(0);
      UpsertRequest namespace2Upsert = upsertRequest.getAllValues().get(1);

      assertEquals("namespace-1", namespace1Upsert.getNamespace());
      assertEquals("namespace-2", namespace2Upsert.getNamespace());

      assertEquals(2, namespace1Upsert.getVectorsList().size());
      assertEquals(2, namespace2Upsert.getVectorsList().size());


      // make sure vectors are correct for each document and namespace
      assertEquals(doc0ForNamespace1, namespace1Upsert.getVectorsList().get(0).getValuesList());
      assertEquals(doc1ForNamespace1, namespace1Upsert.getVectorsList().get(1).getValuesList());
      assertEquals(doc0ForNamespace2, namespace2Upsert.getVectorsList().get(0).getValuesList());
      assertEquals(doc1ForNamespace2, namespace2Upsert.getVectorsList().get(1).getValuesList());

    }
  }

  @Test
  public void testCorrectMetadata() throws Exception {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
      Mockito.verify(stub, Mockito.times(2)).upsert(upsertRequest.capture());
      UpsertRequest namespace1Upsert = upsertRequest.getAllValues().get(0);
      UpsertRequest namespace2Upsert = upsertRequest.getAllValues().get(1);


      // make sure metadata is correct
      assertEquals(namespace1Upsert.getVectorsList().get(0).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace1Upsert.getVectorsList().get(0).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace1Upsert.getVectorsList().get(1).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace1Upsert.getVectorsList().get(1).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace2Upsert.getVectorsList().get(0).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data\"\n");
      assertEquals(namespace2Upsert.getVectorsList().get(0).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
      assertEquals(namespace2Upsert.getVectorsList().get(1).getMetadata().getFields().get("metaString1").toString(),
          "string_value: \"some string data 2\"\n");
      assertEquals(namespace2Upsert.getVectorsList().get(1).getMetadata().getFields().get("metaList").toString(),
          "string_value: \"[1, 2, 3]\"\n");
    }
  }

  @Test
  public void testUpsertAndUpdateIncorrectNumEmbeddingFields() throws Exception {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {

      TestMessenger messenger = new TestMessenger();
      Config config = ConfigFactory.load("PineconeIndexerTest/incorrect-fields.conf");
      PineconeIndexer indexer = new PineconeIndexer(config, messenger, "testing");
      TestMessenger messenger2 = new TestMessenger();
      Config config2 = ConfigFactory.load("PineconeIndexerTest/incorrect-fields-update.conf");
      PineconeIndexer indexer2 = new PineconeIndexer(config2, messenger2, "testing");
      indexer2.validateConnection();
      indexer.validateConnection();

      assertThrows(IndexOutOfBoundsException.class, () -> {
        indexer2.sendToIndex(List.of(doc0, doc1));
      });

      assertThrows(IndexOutOfBoundsException.class, () -> {
        indexer.sendToIndex(List.of(doc0, doc1));
      });

    }
  }


  @Test
  public void testUpdateMultipleNamespaces() throws Exception {
    try (MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      Mockito.when(mock.connect("good")).thenReturn(goodConnection);
    })) {

      TestMessenger messenger = new TestMessenger();
      Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces-update.conf");
      PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
      indexerGood.validateConnection();

      messenger.sendForIndexing(doc0);
      messenger.sendForIndexing(doc1);
      indexerGood.run(2);

      ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
      Mockito.verify(stub, Mockito.times(4)).update(updateRequest.capture());
      ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
      Mockito.verify(stub, Mockito.times(0)).upsert(upsertRequest.capture());

      // make sure no upserts were made
      assertTrue(upsertRequest.getAllValues().isEmpty());

      // make sure two updates were made
      assertTrue(updateRequest.getAllValues().size() == 4);
      UpdateRequest namespace1Request1 = updateRequest.getAllValues().get(0);
      UpdateRequest namespace1Request2 = updateRequest.getAllValues().get(1);
      UpdateRequest namespace2Request1 = updateRequest.getAllValues().get(2);
      UpdateRequest namespace2Request2 = updateRequest.getAllValues().get(3);

      assertEquals("namespace-1", namespace1Request1.getNamespace());
      assertEquals("namespace-1", namespace1Request2.getNamespace());
      assertEquals("namespace-2", namespace2Request1.getNamespace());
      assertEquals("namespace-2", namespace2Request2.getNamespace());


      // make sure vectors are correct for each document and namespace
      assertEquals(doc0ForNamespace1, namespace1Request1.getValuesList());
      assertEquals(doc1ForNamespace1, namespace1Request2.getValuesList());
      assertEquals(doc0ForNamespace2, namespace2Request1.getValuesList());
      assertEquals(doc1ForNamespace2, namespace2Request2.getValuesList());

    }
  }
}
