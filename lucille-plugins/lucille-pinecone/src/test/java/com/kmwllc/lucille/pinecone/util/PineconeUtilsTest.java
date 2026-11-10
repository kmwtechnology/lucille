package com.kmwllc.lucille.pinecone.util;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import org.junit.Test;
import org.openapitools.db_control.client.model.IndexModel;
import org.openapitools.db_control.client.model.IndexModelStatus;

public class PineconeUtilsTest {
  @Test
  public void testIsClientStable() {
    Pinecone mockClient = mock(Pinecone.class);
    IndexModel model = mock(IndexModel.class);
    IndexModelStatus status = mock(IndexModelStatus.class);
    when(model.getStatus()).thenReturn(status);
    when(status.getState()).thenReturn("Initializing").thenReturn("Terminating").thenReturn("InitializationFailed")
        .thenReturn("Ready").thenReturn("Ready");
    when(mockClient.describeIndex(anyString())).thenReturn(model);

    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertFalse(PineconeUtils.isClientStable(mockClient, "name"));
    assertTrue(PineconeUtils.isClientStable(mockClient, "name"));
    assertTrue(PineconeUtils.isClientStable(mockClient, "name"));
  }

  @Test
  public void testDefaultNamespaceIsPineconesDefault() {
    assertEquals("", PineconeUtils.getDefaultNamespace());
  }

  // Index.list(namespace, prefix) runs this validation, with the namespace required. It rejects an empty namespace in
  // some versions of the client, and mocking Index in other tests would hide that.
  @Test
  public void testClientAcceptsDefaultNamespaceWhenListing() {
    Index index = mock(Index.class, CALLS_REAL_METHODS);
    index.validateListEndpointParameters(PineconeUtils.getDefaultNamespace(), "prefix", null, null, true, true, false, false);
  }
}
