package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.pinecone.clients.Pinecone;
import org.junit.Test;
import org.openapitools.control.client.model.IndexModel;
import org.openapitools.control.client.model.IndexModelStatus;
import org.openapitools.control.client.model.IndexModelStatus.StateEnum;

public class PineconeUtilsTest {
  @Test
  public void testIsClientStable() {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel model = mock(IndexModel.class);
    IndexModelStatus status = mock(IndexModelStatus.class);
    when(model.getStatus()).thenReturn(status);
    when(status.getState()).thenReturn(StateEnum.INITIALIZING).thenReturn(StateEnum.TERMINATING).thenReturn(StateEnum.INITIALIZATIONFAILED)
        .thenReturn(StateEnum.READY).thenReturn(StateEnum.READY);
    when(mockClient.describeIndex(anyString())).thenReturn(model);

    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertTrue(PineconeUtils.isClientStable(mockClient, "name"));
    assertTrue(PineconeUtils.isClientStable(mockClient, "name"));
  }
}

