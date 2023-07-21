package com.kmwllc.lucille.core;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaDocumentTest {

  @Test
  public void testEqualsAndHashcode() throws Exception {

    String json1 = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}";
    String json2 = "{\"id\":\"123\", \"field2\":\"val2\", \"field1\":\"val1\" }";
    String json3 = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val3\"}";

    KafkaDocument doc1 = new KafkaDocument(new ConsumerRecord("topic1", 0, 0, "123", json1));
    KafkaDocument doc2 = new KafkaDocument(new ConsumerRecord("topic1", 0, 0, "123", json2));
    KafkaDocument doc3 = new KafkaDocument(new ConsumerRecord("topic1", 0, 0, "123", json3));

    KafkaDocument doc4 = new KafkaDocument(new ConsumerRecord("topic1a", 0, 0, "123", json1));
    KafkaDocument doc5 = new KafkaDocument(new ConsumerRecord("topic1", 1, 0, "123", json1));
    KafkaDocument doc6 = new KafkaDocument(new ConsumerRecord("topic1", 0, 1, "123", json1));
    KafkaDocument doc7 = new KafkaDocument(new ConsumerRecord("topic1", 0, 0, "124", json1));

    assertTrue(doc1.equals(doc1));
    assertTrue(doc1.equals(doc2));
    assertTrue(doc2.equals(doc1));
    assertTrue(!doc3.equals(doc1));
    assertTrue(!doc1.equals(doc3));
    assertTrue(!doc1.equals(new Object()));
    assertEquals(doc1.hashCode(), doc2.hashCode());

    assertEquals(doc1.hashCode(), doc1.clone().hashCode());
    assertTrue(doc1.clone().equals(doc1));
    assertTrue(doc1.equals(doc1.clone()));

    assertFalse(doc1.equals(doc4));
    assertFalse(doc1.equals(doc5));
    assertFalse(doc1.equals(doc6));
    assertFalse(doc1.equals(doc7));

    // hashcodes of unequal objects are not required to be unequal, but if these turned out to be
    // equal
    // it would be a cause for concern
    assertNotEquals(doc1.hashCode(), doc3.hashCode());
    assertNotEquals(doc1.hashCode(), doc4.hashCode());
    assertNotEquals(doc1.hashCode(), doc5.hashCode());
    assertNotEquals(doc1.hashCode(), doc6.hashCode());
    assertNotEquals(doc1.hashCode(), doc7.hashCode());
  }
}
