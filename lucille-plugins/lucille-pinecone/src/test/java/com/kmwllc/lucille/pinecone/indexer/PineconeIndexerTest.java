package com.kmwllc.lucille.pinecone.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.theories.suppliers.TestedOn;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.mockito.internal.matchers.Any;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import android.annotation.TargetApi;
import io.grpc.ConnectivityState;
import io.pinecone.PineconeClient;
import io.pinecone.PineconeClientConfig;
import io.pinecone.PineconeConnection;
import io.pinecone.PineconeConnectionConfig;
import io.pinecone.proto.UpdateRequest;
import io.pinecone.proto.UpsertRequest;
import io.pinecone.proto.Vector;
import io.pinecone.proto.VectorServiceGrpc;

public class PineconeIndexerTest {

  private PineconeClient mockClient;
  private VectorServiceGrpc.VectorServiceBlockingStub stub;
  private PineconeConnection goodConnection;


  @Before
  public void setup() {
    setUpPineconeClient();
  }

  private void setUpPineconeClient() {
    mockClient = Mockito.mock(PineconeClient.class);

    stub = Mockito.mock(VectorServiceGrpc.VectorServiceBlockingStub.class);


    goodConnection = Mockito.mock(PineconeConnection.class);
    Mockito.when(goodConnection.getChannel().getState(true)).thenReturn(ConnectivityState.READY);
    Mockito.when(goodConnection.getBlockingStub()).thenReturn(stub);

    PineconeConnection transientFailureConnection = Mockito.mock(PineconeConnection.class);
    Mockito.when(transientFailureConnection.getChannel().getState(true)).thenReturn(ConnectivityState.TRANSIENT_FAILURE);
    PineconeConnection shutdownConnection = Mockito.mock(PineconeConnection.class);
    Mockito.when(shutdownConnection.getChannel().getState(true)).thenReturn(ConnectivityState.SHUTDOWN);


    Mockito.when(mockClient.connect("good")).thenReturn(goodConnection);
    Mockito.when(mockClient.connect("failure")).thenReturn(transientFailureConnection);
    Mockito.when(mockClient.connect("shutdown")).thenReturn(shutdownConnection);

  }
  
  @Test 
  public void testClientCreatedWithCorrectConfig() {
    Map<PineconeClient, List<Object>> constructorArgs = new HashMap<>();

    try(MockedConstruction<PineconeClient> client = Mockito.mockConstruction(PineconeClient.class, (mock, context) -> {
      constructorArgs.put(mock, new ArrayList<>(context.arguments()));
    })) {
      Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
      TestMessenger messenger = new TestMessenger();
      new PineconeIndexer(configGood, messenger, "testing");

      assertTrue(client.constructed().size() == 1);
      PineconeClient constructed = client.constructed().get(0);
      assertTrue(constructorArgs.get(constructed) instanceof PineconeClientConfig);

      PineconeClientConfig config = (PineconeClientConfig) constructorArgs.get(constructed);

      assertEquals("apiKey", config.getApiKey());
      assertEquals("enviroment", config.getEnvironment());
      assertEquals("projectName", config.getProjectName());
      assertEquals(100, config.getServerSideTimeoutSec());

    }

  }

  @Test
  public void testValidateConnection() {
    TestMessenger messenger = new TestMessenger();
    Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");
    Config configFailure = ConfigFactory.load("PineconeIndexerTest/failure-config.conf");
    Config configShutdown = ConfigFactory.load("PineconeIndexerTest/shutdown-config.conf");

    PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
    PineconeIndexer indexerFailure = new PineconeIndexer(configFailure, messenger, "testing");
    PineconeIndexer indexerShutdown = new PineconeIndexer(configShutdown, messenger, "testing");

    indexerGood.validateConnection();
    Mockito.verify(mockClient, Mockito.times(1)).connect("good");
    indexerGood.validateConnection();
    Mockito.verify(mockClient, Mockito.times(1)).connect("good");

    assertTrue(indexerGood.validateConnection());
    assertFalse(indexerFailure.validateConnection());
    assertFalse(indexerShutdown.validateConnection());
  }

  @Test
  public void testCloseConnection() {
    TestMessenger messenger = new TestMessenger();
    Config configGood = ConfigFactory.load("PineconeIndexerTest/good-config.conf");

    PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");
    indexerGood.closeConnection();
    Mockito.verify(goodConnection, Mockito.times(0)).close();
    indexerGood.validateConnection();
    indexerGood.closeConnection();
    Mockito.verify(goodConnection, Mockito.times(1)).close();
  }



  @Test
  public void testUpsertNoNamespacesProvided() throws Exception {
    ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
    Mockito.verify(stub.update(updateRequest.capture()));
    ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
    Mockito.verify(stub.upsert(upsertRequest.capture()));

    TestMessenger messenger = new TestMessenger();
    Config configGood = ConfigFactory.load("PineconeIndexerTest/no-namespaces.conf");

    PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");

    indexerGood.validateConnection();
    Document doc0 = Document.create("doc0");
    Document doc1 = Document.create("doc1");
    doc0.update("doc0", UpdateMode.OVERWRITE, 1.0, 2.0);
    doc1.update("doc1", UpdateMode.OVERWRITE, 3.0, 4.0);

    messenger.sendForIndexing(doc0);
    messenger.sendForIndexing(doc1);
    indexerGood.run(2);


    //make sure no upserts or updates have happened 
    assertTrue(upsertRequest.getAllValues().isEmpty());
    assertTrue(updateRequest.getAllValues().isEmpty());
  }

  @Test
  public void testUpsertTwoNamespace() throws Exception {
    ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
    Mockito.verify(stub.update(updateRequest.capture()));
    ArgumentCaptor<UpsertRequest> upsertRequest = ArgumentCaptor.forClass(UpsertRequest.class);
    Mockito.verify(stub.upsert(upsertRequest.capture()));

    TestMessenger messenger = new TestMessenger();
    Config configGood = ConfigFactory.load("PineconeIndexerTest/two-namespaces.conf");

    PineconeIndexer indexerGood = new PineconeIndexer(configGood, messenger, "testing");

    indexerGood.validateConnection();
    Document doc0 = Document.create("doc0");
    Document doc1 = Document.create("doc1");
    doc0.update("vector", UpdateMode.OVERWRITE, 1.0, 2.0);
    doc0.update("metaString1", UpdateMode.OVERWRITE, "some string data");
    doc0.update("metaString2", UpdateMode.OVERWRITE, "some more string data");
    doc0.update("metaList", UpdateMode.OVERWRITE, 1, 2, 3);
    doc1.update("vector", UpdateMode.OVERWRITE, 3.0, 4.0);
    doc1.update("metaString1", UpdateMode.OVERWRITE, "some string data");
    doc1.update("metaString2", UpdateMode.OVERWRITE, "some more string data");
    doc1.update("metaList", UpdateMode.OVERWRITE, 1, 2, 3);

    messenger.sendForIndexing(doc0);
    messenger.sendForIndexing(doc1);
    indexerGood.run(2);

    //make sure no upserts or updates have happened 
    assertTrue(upsertRequest.getAllValues().isEmpty());
    assertTrue(updateRequest.getAllValues().isEmpty());
  }


  

  @Test
  public void sendToIndexUpdate() {

    ArgumentCaptor<UpdateRequest> updateRequest = ArgumentCaptor.forClass(UpdateRequest.class);
    Mockito.verify(stub.update(updateRequest.capture()));
  }
}
