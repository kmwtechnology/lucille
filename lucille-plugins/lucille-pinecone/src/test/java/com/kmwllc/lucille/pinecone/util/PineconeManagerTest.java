package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import io.pinecone.clients.Pinecone;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.openapitools.control.client.model.IndexModel;
import org.openapitools.control.client.model.IndexModelStatus;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;

public class PineconeManagerTest {

  @Test
  public void testReuseClient() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      when(mock.build()).thenReturn(mockClient);
    })) {
      // test that manager is retrieving the same client object if given same apiKey, and that .build was only called three times
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("differentClient");
      PineconeManager.getClient("differentClient");
      PineconeManager.getClient("anotherClient");
      // build should have constructed three times for sameClient, differentClient and anotherClient
      assertEquals(3, builder.constructed().size());
    }
  }

  @Test
  public void testSuccessBuildingClient() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      Pinecone mockClient = mock(Pinecone.class);
      when(mock.build()).thenReturn(mockClient);
    })) {
      try {
        Pinecone client = PineconeManager.getClient("clientSuccess");
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void testFailureBuildingClient() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      when(mock.build()).thenThrow(Exception.class);
    })) {
      assertThrows(Exception.class, () -> PineconeManager.getClient("clientFail"));
    }
  }

  @Test
  public void testIsStable() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      Pinecone mockClient = mock(Pinecone.class);
      when(mock.build()).thenReturn(mockClient);
      IndexModel model = mock(IndexModel.class);
      IndexModelStatus status = mock(IndexModelStatus.class);
      when(model.getStatus()).thenReturn(status);
      when(status.getState()).thenReturn(StateEnum.INITIALIZING).thenReturn(StateEnum.TERMINATING).thenReturn(StateEnum.INITIALIZATIONFAILED)
              .thenReturn(StateEnum.READY).thenReturn(StateEnum.READY);
      when(mockClient.describeIndex(anyString())).thenReturn(model);
    })) {
      // create a client within the clients map
      Pinecone client1 = PineconeManager.getClient("client1");
      // now should return false 3 times and true once
      assertFalse(PineconeManager.isStable("client1", "name"));
      assertFalse(PineconeManager.isStable("client1", "name"));
      assertFalse(PineconeManager.isStable("client1", "name"));
      assertTrue(PineconeManager.isStable("client1", "name"));

      // when a client has not been created should be false, even though StateEnum.READY
      assertFalse(PineconeManager.isStable("anotherClient", "name"));
    }
  }
}

