package com.kmwllc.lucille.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.indexer.NopIndexer;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

/**
 * Direct tests for {@link Indexer#getConvertedIndexerDoc} and {@link Indexer#getRawIndexerDoc}.
 */
public class IndexerDocTest {

  private static Indexer indexer(String indexerConfig) {
    return new NopIndexer(ConfigFactory.parseString("indexer {" + indexerConfig + "}"), new TestMessenger(), false, "test");
  }

  private static Document parentWithChild() throws Exception {
    Document doc = Document.createFromJson("{\"id\": \"doc1\", \"str\": \"s\", \"int\": 1, \"list\": [\"a\", \"b\"], "
        + "\"obj\": {\"k\": \"v\"}}");
    doc.setField("bytes", new byte[] {1, 2, 3});
    doc.addChild(Document.create("child1"));
    return doc;
  }

  @Test
  public void testConvertedDocHasPlainJavaValues() throws Exception {
    Map<String, Object> map = indexer("").getConvertedIndexerDoc(parentWithChild());

    assertEquals("doc1", map.get("id"));
    assertEquals("s", map.get("str"));
    assertEquals(1, map.get("int"));
    assertEquals(List.of("a", "b"), map.get("list"));
    assertEquals(Map.of("k", "v"), map.get("obj"));
    assertArrayEquals(new byte[] {1, 2, 3}, (byte[]) map.get("bytes"));
  }

  @Test
  public void testConvertedDocAlwaysOmitsChildren() throws Exception {
    assertFalse(indexer("").getConvertedIndexerDoc(parentWithChild()).containsKey(Document.CHILDREN_FIELD));
    assertFalse(indexer("whitelist: [\"id\", \"" + Document.CHILDREN_FIELD + "\"]")
        .getConvertedIndexerDoc(parentWithChild()).containsKey(Document.CHILDREN_FIELD));
  }

  @Test
  public void testConvertedDocAppliesFieldFilter() throws Exception {
    assertEquals(Map.of("id", "doc1", "str", "s"),
        indexer("whitelist: [\"id\", \"str\"]").getConvertedIndexerDoc(Document.createFromJson(
            "{\"id\": \"doc1\", \"str\": \"s\", \"int\": 1}")));

    assertEquals(Map.of("id", "doc1", "int", 1),
        indexer("blacklist: [\"str\"]").getConvertedIndexerDoc(Document.createFromJson(
            "{\"id\": \"doc1\", \"str\": \"s\", \"int\": 1}")));

    // with both, a field must be whitelisted and not blacklisted
    assertEquals(Map.of("id", "doc1"),
        indexer("whitelist: [\"id\", \"str\"], blacklist: [\"str\"]").getConvertedIndexerDoc(Document.createFromJson(
            "{\"id\": \"doc1\", \"str\": \"s\", \"int\": 1}")));
  }

  @Test
  public void testRawDocUsesTheDocumentsOwnNodes() throws Exception {
    Document doc = parentWithChild();
    Map<String, Object> map = indexer("").getRawIndexerDoc(doc);

    JsonNode data = ((JsonDocument) doc).data;
    for (String field : List.of("id", "str", "int", "list", "obj", "bytes")) {
      assertSame(field, data.get(field), map.get(field));
    }
  }

  @Test
  public void testRawDocAlwaysOmitsChildren() throws Exception {
    assertFalse(indexer("").getRawIndexerDoc(parentWithChild()).containsKey(Document.CHILDREN_FIELD));
    assertFalse(indexer("whitelist: [\"id\", \"" + Document.CHILDREN_FIELD + "\"]")
        .getRawIndexerDoc(parentWithChild()).containsKey(Document.CHILDREN_FIELD));
  }

  @Test
  public void testRawDocAppliesFieldFilter() throws Exception {
    Map<String, Object> map = indexer("whitelist: [\"id\", \"str\"], blacklist: [\"str\"]").getRawIndexerDoc(parentWithChild());
    assertEquals(List.of("id"), List.copyOf(map.keySet()));
  }

  // a mapper may leave out a null map value but writes nulls inside a JsonNode, so null-containing values are converted
  @Test
  public void testRawDocConvertsValuesContainingNull() throws Exception {
    Document doc = Document.createFromJson("{\"id\": \"doc1\", \"str\": \"s\", "
        + "\"withNull\": {\"a\": 1, \"b\": null}, \"listWithNull\": [1, null], \"nullField\": null}");
    Indexer indexer = indexer("");

    Map<String, Object> raw = indexer.getRawIndexerDoc(doc);
    Map<String, Object> converted = indexer.getConvertedIndexerDoc(doc);

    for (String field : List.of("withNull", "listWithNull", "nullField")) {
      assertFalse(field, raw.get(field) instanceof JsonNode);
      assertEquals(field, converted.get(field), raw.get(field));
    }
    assertTrue(raw.containsKey("nullField"));
    // a value without nulls is still the document's own node
    assertSame(((JsonDocument) doc).data.get("str"), raw.get("str"));
  }

  @Test
  public void testRawDocForNonJsonDocumentIsConvertedDoc() throws Exception {
    Document doc = new HashMapDocument("doc1");
    doc.setField("str", "s");
    doc.setField("int", 1);
    doc.addToField("list", "a");
    doc.addToField("list", "b");
    doc.setField("bytes", new byte[] {1, 2, 3});
    doc.addChild(new HashMapDocument("child1"));
    Indexer indexer = indexer("blacklist: [\"int\"]");

    Map<String, Object> expected = indexer.getConvertedIndexerDoc(doc);
    Map<String, Object> raw = indexer.getRawIndexerDoc(doc);

    assertFalse(raw.containsKey(Document.CHILDREN_FIELD));

    assertEquals(expected.keySet(), raw.keySet());
    for (String field : expected.keySet()) {
      Object value = expected.get(field);
      if (value instanceof byte[]) {
        assertArrayEquals((byte[]) value, (byte[]) raw.get(field));
      } else {
        assertEquals(field, value, raw.get(field));
      }
      assertFalse(field, raw.get(field) instanceof JsonNode);
    }
  }
}
