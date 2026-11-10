package com.kmwllc.lucille.message;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.KafkaDocument;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * Deserializes a Kafka record value into a {@link KafkaDocument}. A value that is not a JSON object, or whose id is
 * invalid, fails with a SerializationException, as Kafka requires of a Deserializer, whose cause is the
 * DocumentException that describes the problem (see "Invalid ids" in {@link Document}).
 */
public class KafkaDocumentDeserializer implements Deserializer<Document> {

  // a stock ObjectMapper rejects any byte[] field over 15,000,000 bytes (~14.3 MiB) that the serializer happily wrote;
  // see JsonDocument.createDocumentMapper()
  private static final ObjectMapper MAPPER = JsonDocument.createDocumentMapper();

  @Override
  public Document deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      JsonNode parsed = MAPPER.readTree(data);
      if (!parsed.isObject()) {
        throw new DocumentException("Document JSON must be an object but was " + JsonDocument.describe(parsed));
      }
      return new KafkaDocument((ObjectNode) parsed);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing document", e);
    }
  }
}
