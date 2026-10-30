package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.JsonDocumentWriter.ValueWriter;
import com.kmwllc.lucille.util.FieldFilter;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.StringWriter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.Mockito;

public class JsonDocumentWriterTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final FieldFilter NO_FILTER = new FieldFilter(ConfigFactory.empty());

  private static String write(Document doc, FieldFilter filter, Map<String, ValueWriter> replacements) throws IOException {
    StringWriter out = new StringWriter();
    try (JsonGenerator generator = MAPPER.createGenerator(out)) {
      JsonDocumentWriter.writeObject(doc, generator, filter, replacements);
    }
    return out.toString();
  }

  @Test
  public void testWritesFieldsInOrder() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("str", "s");
    doc.setField("int", 1);
    doc.setField("float", 1.1f);
    doc.setField("bool", true);
    doc.setField("bytes", new byte[] {1, 2, 3});
    doc.addToField("multi", "a");
    doc.addToField("multi", "b");

    assertEquals("{\"id\":\"doc1\",\"str\":\"s\",\"int\":1,\"float\":1.1,\"bool\":true,\"bytes\":\"AQID\","
        + "\"multi\":[\"a\",\"b\"]}", write(doc, NO_FILTER, Map.of()));
  }

  // Null fields are left out at every depth; nulls inside arrays are kept.
  @Test
  public void testNulls() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("top", (String) null);
    doc.setField("obj", MAPPER.readTree("{\"a\":null,\"b\":{\"c\":null,\"d\":1},\"e\":[null,{\"f\":null,\"g\":2},[null]]}"));

    assertEquals("{\"id\":\"doc1\",\"obj\":{\"b\":{\"d\":1},\"e\":[null,{\"g\":2},[null]]}}",
        write(doc, NO_FILTER, Map.of()));
  }

  @Test
  public void testFieldFilter() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("keep", "k");
    doc.setField("drop", "d");

    FieldFilter blacklist = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("drop"))));
    assertEquals("{\"id\":\"doc1\",\"keep\":\"k\"}", write(doc, blacklist, Map.of()));

    FieldFilter whitelist = new FieldFilter(ConfigFactory.parseMap(Map.of("whitelist", List.of("drop"))));
    assertEquals("{\"drop\":\"d\"}", write(doc, whitelist, Map.of()));
  }

  // Children are never written; a writer that indexes them writes them as a replacement.
  @Test
  public void testChildrenNotWritten() throws Exception {
    Document doc = Document.create("doc1");
    doc.addChild(Document.create("child1"));
    doc.setField("after", "a");

    assertEquals("{\"id\":\"doc1\",\"after\":\"a\"}", write(doc, NO_FILTER, Map.of()));
  }

  // A replacement takes the field's place, even when the field is null; one for a field the document doesn't have, or
  // the filter excludes, is added after the fields, in order.
  @Test
  public void testReplacements() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("nullField", (String) null);
    doc.setField("excluded", "x");
    doc.setField("last", "l");

    Map<String, ValueWriter> replacements = new LinkedHashMap<>();
    replacements.put("added", generator -> generator.writeNumber(1));
    replacements.put("id", generator -> generator.writeString("override"));
    replacements.put("nullField", generator -> generator.writeString("set"));
    replacements.put("excluded", generator -> generator.writeString("again"));

    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("excluded"))));
    assertEquals("{\"id\":\"override\",\"nullField\":\"set\",\"last\":\"l\",\"added\":1,\"excluded\":\"again\"}",
        write(doc, filter, replacements));
  }

  // The document is written from its own tree, and not modified.
  @Test
  public void testDocumentUnchanged() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("obj", MAPPER.readTree("{\"a\":null}"));
    doc.addChild(Document.create("child1"));
    Document before = doc.deepCopy();

    write(doc, NO_FILTER, Map.of("id", generator -> generator.writeString("other")));
    assertEquals(before, doc);
  }

  @Test
  public void testRequiresJsonDocument() {
    Document doc = Mockito.mock(Document.class);
    assertFalse(JsonDocumentWriter.canWrite(doc));
    assertTrue(JsonDocumentWriter.canWrite(Document.create("doc1")));
    assertThrows(IllegalArgumentException.class, () -> write(doc, NO_FILTER, Map.of()));
  }
}
