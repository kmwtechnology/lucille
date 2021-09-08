package com.kmwllc.lucille.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class EventTest {

  @Test
  public void testSerializeDeserialize() throws Exception {
    Event event = new Event("docId1", "runId1",
      "message1", Event.Type.CREATE);
    Event event2 = Event.fromJsonString(event.toString());

    assertEquals("docId1", event2.getDocumentId());
    assertEquals("runId1", event2.getRunId());
    assertEquals("message1", event2.getMessage());
    assertEquals(Event.Type.CREATE, event2.getType());
    assertTrue(event.equals(event2));
    assertEquals(event.hashCode(), event2.hashCode());
  }


}
