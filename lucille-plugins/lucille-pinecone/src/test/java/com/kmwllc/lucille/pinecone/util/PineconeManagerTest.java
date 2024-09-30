package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.openapitools.control.client.model.IndexModel;
import org.openapitools.control.client.model.IndexModelStatus;

public class PineconeManagerTest {

  private Index mockIndex;

  private IndexModel mockDescription;

  private IndexModelStatus mockStatus;

  @Before
  public void setUp() {
    mockIndex = mock(Index.class);
    mockDescription = mock(IndexModel.class);
    mockStatus = mock(IndexModelStatus.class);
  }

  @Test
  public void testFailureApiKey() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      when(mock.build()).thenThrow(Exception.class);
    })) {
      assertThrows(Exception.class, () ->  PineconeManager.getClientInstance("apiKeyFail"));
    }
  }

  @Test
  public void testSuccessApiKey() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      Pinecone mockClient = mock(Pinecone.class);
      when(mock.build()).thenReturn(mockClient);
    })) {
      try {
        Pinecone client = PineconeManager.getClientInstance("apiKeySuccess");
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void testReuseClient() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      Pinecone mockClient = mock(Pinecone.class);
      Pinecone differentClient = mock(Pinecone.class);
      // will return mockClient first time is called, then return a different client when called a second time
      when(mock.build()).thenReturn(mockClient).thenReturn(differentClient);
    })) {
      // test that sameClient is retrieving the same client object, while differentClient is getting the differentClient
      Pinecone client = PineconeManager.getClientInstance("sameClient");
      Pinecone client2 = PineconeManager.getClientInstance("sameClient");
      Pinecone client3 = PineconeManager.getClientInstance("differentClient");
      assertSame(client, client2);
      assertNotSame(client, client3);
      assertNotSame(client2, client3);
    }
  }

  @Test
  public void testSuccessGettingClient() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      Pinecone mockClient = mock(Pinecone.class);
      when(mock.build()).thenReturn(mockClient);
    })) {
      try {
        Pinecone client = PineconeManager.getClientInstance("indexConnectionPass");
      } catch (Exception e) {
        fail();
      }
    }
  }

  @Test
  public void testFailureGettingClient() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      when(mock.build()).thenThrow(Exception.class);
    })) {
      assertThrows(Exception.class, () -> PineconeManager.getClientInstance("indexConnectionFail"));
    }
  }
}

