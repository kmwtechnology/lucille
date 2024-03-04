package com.kmwllc.lucille.core;

import com.kmwllc.lucille.message.KafkaDocumentDeserializer;
import com.kmwllc.lucille.message.KafkaDocumentSerializer;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.Test;

import static org.junit.Assert.*;

public class KafkaDocumentTest {

  @Test
  public void testEqualsAndHashcode() throws Exception {

    String json1 = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val2\"}";
    String json2 = "{\"id\":\"123\", \"field2\":\"val2\", \"field1\":\"val1\" }";
    String json3 = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":\"val3\"}";

    KafkaDocument doc1 = new KafkaDocument(
        new ConsumerRecord("topic1", 0, 0, "123", json1));
    KafkaDocument doc2 = new KafkaDocument(
        new ConsumerRecord("topic1", 0, 0, "123", json2));
    KafkaDocument doc3 = new KafkaDocument(
        new ConsumerRecord("topic1", 0, 0, "123", json3));

    KafkaDocument doc4 = new KafkaDocument(
        new ConsumerRecord("topic1a", 0, 0, "123", json1));
    KafkaDocument doc5 = new KafkaDocument(
        new ConsumerRecord("topic1", 1, 0, "123", json1));
    KafkaDocument doc6 = new KafkaDocument(
        new ConsumerRecord("topic1", 0, 1, "123", json1));
    KafkaDocument doc7 = new KafkaDocument(
        new ConsumerRecord("topic1", 0, 0, "124", json1));

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

    // hashcodes of unequal objects are not required to be unequal, but if these turned out to be equal
    // it would be a cause for concern
    assertNotEquals(doc1.hashCode(), doc3.hashCode());
    assertNotEquals(doc1.hashCode(), doc4.hashCode());
    assertNotEquals(doc1.hashCode(), doc5.hashCode());
    assertNotEquals(doc1.hashCode(), doc6.hashCode());
    assertNotEquals(doc1.hashCode(), doc7.hashCode());

  }

  @Test
  public void testGettersAndSetters() throws Exception {
    String json = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":[1.2, 3.4, 5.6]}";
    KafkaDocument doc = new KafkaDocument(
        new ConsumerRecord("topic1", 2, 3, "key1", json));

    // getters specific to KafkaDocument
    assertEquals("topic1", doc.getTopic());
    assertEquals(2, doc.getPartition());
    assertEquals(3, doc.getOffset());
    assertEquals("key1", doc.getKey());

    // Document getters for accessing field values
    assertEquals("123", doc.getId());
    assertEquals("val1", doc.getString("field1"));
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), doc.getFloatList("field2").toArray());

    String json2 = "{\"id\":\"456\", \"field1\":\"val10\"}";
    ConsumerRecord consumerRecord =
        new ConsumerRecord("topic2", 12, 13, "key2", json2);

    // setKafkaMetadata() is intended to be called with the ConsumerRecord that contained the
    // document's json; here we are calling setKafkaMetadata() with a ConsumerRecord for a different
    // document, which is a nonstandard usage, but it allows us to test the effect of the setter
    doc.setKafkaMetadata(consumerRecord);

    // getters specific to KafkaDocument: the kafka metadata is updated
    assertEquals("topic2", doc.getTopic());
    assertEquals(12, doc.getPartition());
    assertEquals(13, doc.getOffset());
    assertEquals("key2", doc.getKey());

    // Document getters for accessing field values: the documents field values are unchanged
    assertEquals("123", doc.getId());
    assertEquals("val1", doc.getString("field1"));
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), doc.getFloatList("field2").toArray());
  }

  @Test
  public void testSerdeWithRegularDocument() throws Exception {
    KafkaDocumentSerializer serializer = new KafkaDocumentSerializer();
    KafkaDocumentDeserializer deserializer = new KafkaDocumentDeserializer();

    String json = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":[1.2, 3.4, 5.6]}";

    // the typical use case for KafkaDocumentSerializer is to serialize a regular (non-Kafka) Document
    // that we want to put onto a Kafka topic, not to serialize a KafkaDocument that has already been
    // retrieved from Kafka
    Document originalDoc = Document.createFromJson(json);
    assertFalse(originalDoc instanceof KafkaDocument);
    assertEquals("123", originalDoc.getId());
    assertEquals("val1", originalDoc.getString("field1"));
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), originalDoc.getFloatList("field2").toArray());

    byte[] docBytes = serializer.serialize("topic", originalDoc);

    Document deserializedDoc = deserializer.deserialize("topic", docBytes);

    assertEquals("123", deserializedDoc.getId());
    assertEquals("val1", deserializedDoc.getString("field1"));
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), deserializedDoc.getFloatList("field2").toArray());
    assertEquals(originalDoc.asMap(), deserializedDoc.asMap());

    // the deserialized document will be a KafkaDocument but the Kafka metadata will not be set
    assertTrue(deserializedDoc instanceof KafkaDocument);
    KafkaDocument kafkaDocument = (KafkaDocument)deserializedDoc;
    assertEquals(null, kafkaDocument.getTopic());
    assertEquals(0, kafkaDocument.getPartition());
    assertEquals(0, kafkaDocument.getOffset());
    assertEquals(null, kafkaDocument.getKey());
  }

  @Test
  public void testSerdeWithKafkaDocument() throws Exception {
    KafkaDocumentSerializer serializer = new KafkaDocumentSerializer();
    KafkaDocumentDeserializer deserializer = new KafkaDocumentDeserializer();

    String json = "{\"id\":\"123\", \"field1\":\"val1\", \"field2\":[1.2, 3.4, 5.6]}";

    // the typical use case for KafkaDocumentSerializer is to serialize a regular (non-Kafka) Document
    // that we want to put onto a Kafka topic, not to serialize a KafkaDocument that has already been
    // retrieved from Kafka
    // however, because KafkaDocument extends Document this usage
    // is not prohibited; this test illustrates what happens when we serialize and
    // deserialize a KafkaDocument: it "works" but the Kafka metadata is lost
    KafkaDocument originalDoc = new KafkaDocument(
        new ConsumerRecord("topic1", 2, 3, "key1", json));
    assertEquals("123", originalDoc.getId());
    assertEquals("val1", originalDoc.getString("field1"));
    assertEquals("topic1", originalDoc.getTopic());
    assertEquals(2, originalDoc.getPartition());
    assertEquals(3, originalDoc.getOffset());
    assertEquals("key1", originalDoc.getKey());
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), originalDoc.getFloatList("field2").toArray());

    byte[] docBytes = serializer.serialize("topic", originalDoc);

    KafkaDocument deserializedDoc = (KafkaDocument)deserializer.deserialize("topic", docBytes);

    assertEquals("123", deserializedDoc.getId());
    assertEquals("val1", deserializedDoc.getString("field1"));
    assertArrayEquals(List.of(1.2f, 3.4f, 5.6f).toArray(), deserializedDoc.getFloatList("field2").toArray());

    // Kafka-specific metadata from the original KafkaDocument is not retained
    assertEquals(null, deserializedDoc.getTopic());
    assertEquals(0, deserializedDoc.getPartition());
    assertEquals(0, deserializedDoc.getOffset());
    assertEquals(null, deserializedDoc.getKey());
    assertEquals(originalDoc.asMap(), deserializedDoc.asMap());
  }
}
