package com.kmwllc.lucille.core;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class RunResultTest {

  @Test
  public void testSuccessWithFailingDocs() {
    RunResult result = new RunResult(true, List.of(connector("c1"), connector("c2")),
        List.of(connectorResult(true, false), connectorResult(true, true)), "run1");

    assertTrue(result.getStatus());
    assertTrue(result.hasFailingDocs());
    assertTrue(result.getMessage().contains("RUN SUMMARY: Partial success."));
    assertTrue(result.getMessage().contains("Some docs failed."));
  }

  @Test
  public void testSuccessWithoutFailingDocs() {
    RunResult result = new RunResult(true, List.of(connector("c1")), List.of(connectorResult(true, false)), "run1");

    assertTrue(result.getStatus());
    assertFalse(result.hasFailingDocs());
    assertTrue(result.getMessage().contains("RUN SUMMARY: Success."));
    assertTrue(result.getMessage().contains("All published docs succeeded."));
  }

  @Test
  public void testFailureWithFailingDocs() {
    RunResult result = new RunResult(false, List.of(connector("c1")), List.of(connectorResult(false, true)),
        Collections.emptyMap(), "run1");

    assertFalse(result.getStatus());
    assertTrue(result.hasFailingDocs());
    assertTrue(result.getMessage().contains("RUN SUMMARY: Failure."));
  }

  @Test
  public void testNoConnectorResults() {
    RunResult result = new RunResult(false, Collections.emptyList(), Collections.emptyList(), null, "run1");

    assertFalse(result.getStatus());
    assertFalse(result.hasFailingDocs());
    assertTrue(result.getMessage().contains("No docs published."));
  }

  private static Connector connector(String name) {
    Connector connector = mock(Connector.class);
    when(connector.getName()).thenReturn(name);
    return connector;
  }

  private static ConnectorResult connectorResult(boolean status, boolean failingDocs) {
    ConnectorResult result = mock(ConnectorResult.class);
    when(result.getStatus()).thenReturn(status);
    when(result.hasFailingDocs()).thenReturn(failingDocs);
    when(result.hasDocs()).thenReturn(true);
    when(result.toString()).thenReturn("connector result");
    return result;
  }
}
