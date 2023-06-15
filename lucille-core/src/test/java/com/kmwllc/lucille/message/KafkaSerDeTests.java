package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Document;
import org.junit.Test;

public class KafkaSerDeTests {
  @Test
  public void test() {
    Document doc = Document.create("test1");
    doc.setField("foo", "bar");
    doc.setField("integervalue", 1);
    KafkaDocumentSerializer serializer = new KafkaDocumentSerializer();
    KafkaDocumentDeserializer deserializer = new KafkaDocumentDeserializer();
    byte[] serialized = serializer.serialize("test", doc);
    Document deserialized = deserializer.deserialize("test", serialized);
    System.out.println(doc.toString());
    System.out.println(deserialized.toString());
  }
}
