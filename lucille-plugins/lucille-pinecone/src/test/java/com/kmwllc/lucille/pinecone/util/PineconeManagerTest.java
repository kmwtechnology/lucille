package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import io.pinecone.clients.Pinecone;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class PineconeManagerTest {

  @Test
  public void testReuseClient() throws Exception {
    Pinecone mockClient = mock(Pinecone.class);
    Pinecone differentClient = mock(Pinecone.class);
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      // will return mockClient first time is called, then return a different client when called a second time
      when(mock.build()).thenReturn(mockClient).thenReturn(differentClient);
    })) {
      // test that "sameClient" is retrieving the same client object, and that .build was only called trice
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("sameClient");
      PineconeManager.getClient("differentClient");
      PineconeManager.getClient("differentClient");
      PineconeManager.getClient("anotherClient");
      // build should have constructed trice for sameClient, differentClient and anotherClient
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
}

