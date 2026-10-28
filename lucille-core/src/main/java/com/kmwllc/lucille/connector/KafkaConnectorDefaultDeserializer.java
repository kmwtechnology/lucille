package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class KafkaConnectorDefaultDeserializer implements Deserializer<Document> {
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private String idField;
  private String docIdPrefix;

  @Override
  public void configure(Map<String, ?> configs, boolean isKey) {
    this.idField = (String) configs.get("idField");
    this.docIdPrefix = (String) configs.get("docIdPrefix");
    if (this.docIdPrefix == null) {
      this.docIdPrefix = "";
    }
  }

  @Override
  public Document deserialize(String topic, byte[] data) {
    if (data == null) {
      return null;
    }

    try {
      JsonNode node = MAPPER.readTree(data);
      if (!node.isObject()) {
        throw new SerializationException("Consumer record value is not a JSON object");
      }
      ObjectNode objectNode = (ObjectNode) node;

      // use the id field if it has a value, otherwise fall back to the Lucene Document.ID_FIELD. A JSON null counts as
      // no value: asText() would turn it into the text "null", which would become the document id.
      String rawId = null;
      if (idField != null && objectNode.hasNonNull(idField)) {
        rawId = objectNode.get(idField).asText();
      } else if (objectNode.hasNonNull(Document.ID_FIELD)) {
        rawId = objectNode.get(Document.ID_FIELD).asText();
      }

      // use the configured docIdPrefix if it exists
      if (rawId != null) {
        objectNode.put(Document.ID_FIELD, docIdPrefix + rawId);
      }

      return Document.create(objectNode);
    } catch (Exception e) {
      throw new SerializationException("Error deserializing document", e);
    }
  }
}
