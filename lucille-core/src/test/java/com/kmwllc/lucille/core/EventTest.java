package com.kmwllc.lucille.core;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.time.Instant;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(JUnit4.class)
public class EventTest {

  @Test
  public void testCreateEqualsHashcodeDeserialize() throws Exception {
    Document doc1 = Document.create("id1", "runId1");
    Event event1 = new Event(doc1, "test message", Event.Type.FINISH);
    Event event1B = Event.fromJsonString(event1.toString());
    assertTrue(event1.equals(event1));
    assertTrue(event1.equals(event1B));
    assertTrue(event1B.equals(event1));
    assertEquals(event1.hashCode(), event1B.hashCode());

    KafkaDocument doc2 = new KafkaDocument(doc1, "t1", 5, 3,"testKey");
    Instant beforeEvent2 = Instant.now();
    Event event2 = new Event(doc2, "test message", Event.Type.FINISH);
    Instant afterEvent2 = Instant.now();
    assertFalse(event1.equals(event2));
    assertFalse(event2.equals(event1));
    assertNotEquals(event1.hashCode(), event2.hashCode());
    Event event2B = Event.fromJsonString(event2.toString());
    assertTrue(event2.equals(event2));
    assertTrue(event2.equals(event2B));
    assertTrue(event2B.equals(event2));
    assertEquals(event2.hashCode(), event2B.hashCode());

    assertEquals("id1", event2B.getDocumentId());
    assertEquals("runId1", event2B.getRunId());
    assertEquals("test message", event2B.getMessage());
    assertEquals(Event.Type.FINISH, event2B.getType());
    assertTrue(beforeEvent2.isBefore(event2.getInstant()));
    assertTrue(afterEvent2.isAfter(event2.getInstant()));
    assertEquals(event2.getInstant(), event2B.getInstant());
    assertEquals("t1", event2B.getTopic());
    assertEquals(Integer.valueOf(5), event2B.getPartition());
    assertEquals("testKey", event2B.getKey());
  }

  @Test
  public void testIsCreate() throws Exception {
    Document doc1 = Document.create("id1", "runId1");
    Event event1 = new Event(doc1, "test message", Event.Type.CREATE);
    Event event2 = new Event(doc1, "test message", Event.Type.FINISH);
    Event event3 = new Event(doc1, "test message", Event.Type.FAIL);
    Event event4 = new Event(doc1, "test message", Event.Type.DROP);
    assertTrue(event1.isCreate());
    assertFalse(event2.isCreate());
    assertFalse(event3.isCreate());
    assertFalse(event4.isCreate());
  }
}
