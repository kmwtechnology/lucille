package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import org.openapitools.control.client.model.IndexModel;
import org.openapitools.control.client.model.IndexModelStatus;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;

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

  @After
  public void tearDown() {
    PineconeManager.resetInstance();
  }

  @Test
  public void testClientUnstable() {
    try(MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class,(mock,context)-> {
      when(mock.getIndexConnection(anyString())).thenReturn(mockIndex);
      when(mock.describeIndex(anyString())).thenReturn(mockDescription);
      when(mockDescription.getStatus()).thenReturn(mockStatus);
      when(mockStatus.getState()).thenReturn(StateEnum.INITIALIZING, StateEnum.INITIALIZATIONFAILED, StateEnum.TERMINATING);
    })){
      PineconeManager manager = PineconeManager.getInstance("apiKey", "indexName");
      assertFalse(manager.isClientStable());
      assertFalse(manager.isClientStable());
      assertFalse(manager.isClientStable());
    }
  }

  @Test
  public void testClientStable() {
    try(MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class,(mock,context)-> {
      when(mock.getIndexConnection(anyString())).thenReturn(mockIndex);
      when(mock.describeIndex(anyString())).thenReturn(mockDescription);
      when(mockDescription.getStatus()).thenReturn(mockStatus);
      when(mockStatus.getState()).thenReturn(StateEnum.READY, StateEnum.SCALINGDOWN, StateEnum.SCALINGUP,
          StateEnum.SCALINGDOWNPODSIZE, StateEnum.SCALINGUPPODSIZE);
    })){
      PineconeManager manager = PineconeManager.getInstance("apiKey", "indexName");
      assertTrue(manager.isClientStable());
      assertTrue(manager.isClientStable());
      assertTrue(manager.isClientStable());
      assertTrue(manager.isClientStable());
      assertTrue(manager.isClientStable());
    }
  }

  @Test
  public void testFailureApiKey() throws Exception {
    try(MockedConstruction<Pinecone.Builder> builder = Mockito.mockConstruction(Pinecone.Builder.class,(mock,context)-> {
      when(mock.build()).thenThrow(Exception.class);
    })) {
      assertThrows(Exception.class, () ->  PineconeManager.getInstance("apiKey", "indexName"));
    }
  }

  @Test
  public void testSuccessIndexConnection() throws Exception {
    try(MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class,(mock,context)-> {
      when(mock.getIndexConnection(anyString())).thenReturn(mockIndex);
      when(mock.describeIndex(anyString())).thenReturn(mockDescription);
      when(mockDescription.getStatus()).thenReturn(mockStatus);
    })){
      PineconeManager manager = PineconeManager.getInstance("apiKey", "indexName");
      Pinecone mockClient = client.constructed().get(0);

      verify(mockClient, times(1)).getIndexConnection(anyString());
    }
  }

  @Test
  public void testFailureIndexConnection() throws Exception {
    try(MockedConstruction<Pinecone> client = Mockito.mockConstruction(Pinecone.class,(mock,context)-> {
      when(mock.getIndexConnection(anyString())).thenThrow(Exception.class);
      when(mock.describeIndex(anyString())).thenReturn(mockDescription);
      when(mockDescription.getStatus()).thenReturn(mockStatus);
    })){
      assertThrows(Exception.class, () -> PineconeManager.getInstance("apiKey", "indexName"));
    }
  }
}

