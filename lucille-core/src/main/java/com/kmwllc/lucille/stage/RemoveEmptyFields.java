package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Removes empty fields from a document. A field is empty if it is null, an empty string, an empty list, or a list holding
 * only nulls and empty strings. Whitespace-only strings, nested objects and reserved fields are never removed.
 */
public class RemoveEmptyFields extends Stage {

  public static final Spec SPEC = SpecBuilder.stage().build();

  public RemoveEmptyFields(Config config) {
    super(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String fieldName : doc.getFieldNames()) {
      // removeField throws on reserved fields such as the id and attached children
      if (!Document.RESERVED_FIELDS.contains(fieldName) && isEmpty(doc.getJson(fieldName))) {
        doc.removeField(fieldName);
      }
    }
    return null;
  }

  private static boolean isEmpty(JsonNode node) {
    if (!node.isArray()) {
      return isEmptyValue(node);
    }
    for (JsonNode element : node) {
      if (!isEmptyValue(element)) {
        return false;
      }
    }
    return true;
  }

  private static boolean isEmptyValue(JsonNode node) {
    return node.isNull() || (node.isTextual() && node.textValue().isEmpty());
  }
}
