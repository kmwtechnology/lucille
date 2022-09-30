package com.kmwllc.lucille.core;

import static org.junit.Assert.*;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

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

    assertEquals(doc1, doc1);
    assertEquals(doc1, doc2);
    assertEquals(doc2, doc1);
    assertFalse(doc3.equals(doc1));
    assertFalse(doc1.equals(doc3));
    assertFalse(doc1.equals(new Object()));
    assertEquals(doc1.hashCode(), doc2.hashCode());

    assertEquals(doc1.hashCode(), doc1.clone().hashCode());
    assertEquals(doc1.clone(), doc1);
    assertEquals(doc1, doc1.clone());

    assertNotEquals(doc1, doc4);
    assertNotEquals(doc1, doc5);
    assertNotEquals(doc1, doc6);
    assertNotEquals(doc1, doc7);

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
