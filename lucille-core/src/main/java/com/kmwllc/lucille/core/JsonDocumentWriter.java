package com.kmwllc.lucille.core;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.util.FieldFilter;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Writes a {@link JsonDocument}'s fields as a JSON object straight from its backing tree to a Jackson
 * {@link JsonGenerator}, for an indexer whose client serializes its request bodies with Jackson. Nothing is copied: the
 * tree is walked once as it is written, instead of first building a Map of the document with
 * {@link Indexer#getRawIndexerDoc} for the client to serialize.
 *
 * <p>The JSON written is the same as the client's Jackson mapper writes for that Map, when the mapper leaves out null
 * map values, as the OpenSearch and Elasticsearch clients' mappers do:
 * <ul>
 *   <li>fields the field filter excludes, and {@link Document#CHILDREN_FIELD}, are not written;</li>
 *   <li>a field with a null value is not written, at any depth, but a null inside an array is;</li>
 *   <li>a field with a replacement is written with the replacement's value, in the field's position when the document
 *   has the field and the filter includes it, as {@link Map#put} would replace it. Replacements for fields the document
 *   doesn't have are written after its fields, in the replacements' iteration order.</li>
 * </ul>
 *
 * <p>The document must not be modified while it is being written.
 */
public final class JsonDocumentWriter {

  /** Writes a JSON value. */
  @FunctionalInterface
  public interface ValueWriter {
    void write(JsonGenerator generator) throws IOException;
  }

  private JsonDocumentWriter() {
  }

  /** Whether {@link #writeObject} can write the given document: it is backed by a JSON tree. */
  public static boolean canWrite(Document doc) {
    return doc instanceof JsonDocument;
  }

  /**
   * Writes the document as a JSON object, with the rules above.
   *
   * @param doc the document, for which {@link #canWrite} is true.
   * @param generator the generator to write to.
   * @param filter the field filter applied to the document's top-level fields.
   * @param replacements values to write in place of the named top-level fields, or to add.
   * @throws IllegalArgumentException if the document is not backed by a JSON tree.
   * @throws IOException if the generator fails to write.
   */
  public static void writeObject(Document doc, JsonGenerator generator, FieldFilter filter,
      Map<String, ValueWriter> replacements) throws IOException {
    if (!canWrite(doc)) {
      throw new IllegalArgumentException("Document " + doc.getId() + " is not backed by a JSON tree");
    }

    Set<String> replaced = replacements.isEmpty() ? Set.of() : new HashSet<>();
    generator.writeStartObject();
    for (Map.Entry<String, JsonNode> field : ((JsonDocument) doc).data.properties()) {
      String name = field.getKey();
      if (Document.CHILDREN_FIELD.equals(name) || !filter.shouldInclude(name)) {
        continue;
      }
      ValueWriter replacement = replacements.get(name);
      if (replacement != null) {
        generator.writeFieldName(name);
        replacement.write(generator);
        replaced.add(name);
      } else if (!isNull(field.getValue())) {
        generator.writeFieldName(name);
        writeValue(generator, field.getValue());
      }
    }
    for (Map.Entry<String, ValueWriter> replacement : replacements.entrySet()) {
      if (!replaced.contains(replacement.getKey())) {
        generator.writeFieldName(replacement.getKey());
        replacement.getValue().write(generator);
      }
    }
    generator.writeEndObject();
  }

  // a missing node converts to a null map value, which is left out like a null
  private static boolean isNull(JsonNode value) {
    return value.isNull() || value.isMissingNode();
  }

  // Makes the same generator calls as Jackson's own node serializers.
  private static void writeValue(JsonGenerator generator, JsonNode value) throws IOException {
    switch (value.getNodeType()) {
      case STRING:
        generator.writeString(value.textValue());
        break;
      case NUMBER:
        writeNumber(generator, value);
        break;
      case BOOLEAN:
        generator.writeBoolean(value.booleanValue());
        break;
      case NULL:
        // only reached inside an array; null fields are left out
        generator.writeNull();
        break;
      case BINARY:
        generator.writeBinary(value.binaryValue());
        break;
      case OBJECT:
        generator.writeStartObject();
        for (Map.Entry<String, JsonNode> field : value.properties()) {
          if (!isNull(field.getValue())) {
            generator.writeFieldName(field.getKey());
            writeValue(generator, field.getValue());
          }
        }
        generator.writeEndObject();
        break;
      case ARRAY:
        generator.writeStartArray();
        for (JsonNode element : value) {
          writeValue(generator, element);
        }
        generator.writeEndArray();
        break;
      default:
        writeOther(generator, value);
    }
  }

  private static void writeNumber(JsonGenerator generator, JsonNode value) throws IOException {
    switch (value.numberType()) {
      case INT:
        generator.writeNumber(value.intValue());
        break;
      case LONG:
        generator.writeNumber(value.longValue());
        break;
      case BIG_INTEGER:
        generator.writeNumber(value.bigIntegerValue());
        break;
      case FLOAT:
        generator.writeNumber(value.floatValue());
        break;
      case DOUBLE:
        generator.writeNumber(value.doubleValue());
        break;
      case BIG_DECIMAL:
        generator.writeNumber(value.decimalValue());
        break;
      default:
        writeOther(generator, value);
    }
  }

  // A node type that a tree parsed from JSON doesn't contain, such as a POJO node, is written by the generator's own
  // mapper, as the client would write it.
  private static void writeOther(JsonGenerator generator, JsonNode value) throws IOException {
    ObjectCodec codec = generator.getCodec() != null ? generator.getCodec() : JsonDocument.MAPPER;
    codec.writeValue(generator, value);
  }
}
