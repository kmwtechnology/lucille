package com.kmwllc.lucille.api.objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConnectorException;
import java.lang.reflect.InvocationTargetException;
import org.junit.Test;

public class RunErrorTest {

  @Test
  public void testNull() {
    assertNull(RunError.from(null));
  }

  @Test
  public void testNoCause() {
    RunError error = RunError.from(new IllegalStateException("bad state"));

    assertEquals(IllegalStateException.class.getName(), error.getClassName());
    assertEquals("bad state", error.getMessage());
    assertEquals(IllegalStateException.class.getName(), error.getRootCauseClassName());
    assertEquals("bad state", error.getRootCauseMessage());
  }

  // a connector whose constructor throws reaches RunnerManager wrapped like this, with no message of its own
  @Test
  public void testReflectiveWrapper() {
    ConnectorException root = new ConnectorException("Required file not found: /tmp/x");
    RunError error = RunError.from(new InvocationTargetException(root));

    assertEquals(InvocationTargetException.class.getName(), error.getClassName());
    assertNull(error.getMessage());
    assertEquals(ConnectorException.class.getName(), error.getRootCauseClassName());
    assertEquals("Required file not found: /tmp/x", error.getRootCauseMessage());
  }

  @Test
  public void testCauseCycle() {
    Exception a = new Exception("a");
    Exception b = new Exception("b", a);
    a.initCause(b);

    // terminates despite the cycle
    assertTrue(RunError.from(b).getRootCauseMessage().matches("[ab]"));
  }

  @Test
  public void testJsonShape() {
    JsonNode json = new ObjectMapper().valueToTree(RunError.from(new IllegalStateException("bad state")));

    assertEquals(4, json.size());
    assertEquals(IllegalStateException.class.getName(), json.get("className").asText());
    assertEquals("bad state", json.get("message").asText());
    assertEquals(IllegalStateException.class.getName(), json.get("rootCauseClassName").asText());
    assertEquals("bad state", json.get("rootCauseMessage").asText());
  }
}
