package com.kmwllc.lucille.opensearch.indexer;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.POJONode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import jakarta.json.stream.JsonGenerator;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.client.json.JsonpMapper;
import org.opensearch.client.json.JsonpSerializable;
import org.opensearch.client.json.NdJsonpSerializable;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.core.BulkRequest;
import org.opensearch.client.opensearch.core.BulkResponse;
import org.opensearch.client.transport.endpoints.BooleanResponse;

/**
 * Pins the exact bulk request body that OpenSearchIndexer hands to the client, serialized the same way the
 * client's transport serializes it (newline-delimited JSON produced by the default JacksonJsonpMapper).
 */
public class OpenSearchIndexerBulkBodyTest {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private OpenSearchClient mockClient;

  @Before
  public void setup() throws Exception {
    mockClient = Mockito.mock(OpenSearchClient.class);
    BooleanResponse mockBooleanResponse = Mockito.mock(BooleanResponse.class);
    when(mockClient.ping()).thenReturn(mockBooleanResponse);
    when(mockBooleanResponse.value()).thenReturn(true);
    when(mockClient.bulk(any(BulkRequest.class))).thenReturn(Mockito.mock(BulkResponse.class));
  }

  @Test
  public void testScalarFieldsAndNulls() throws Exception {
    Document doc = Document.create("doc1");
    populateScalars(doc);

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + SCALARS_BODY + "\n", bulkBody(config(""), doc));
  }

  @Test
  public void testNestedJson() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("obj", MAPPER.readTree(NESTED_JSON));
    doc.setField("arr", MAPPER.readTree("[1, null, \"x\", {\"k\": null, \"m\": 2}, [null]]"));
    doc.setField("plain", MAPPER.readTree("{\"a\": {\"b\": [1, 2.5, \"c\", true]}}"));

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc1\"," + NESTED_BODY + ",\"arr\":[1,null,\"x\",{\"m\":2},[null]],"
        + "\"plain\":{\"a\":{\"b\":[1,2.5,\"c\",true]}}}\n", bulkBody(config(""), doc));
  }

  @Test
  public void testNestedFloat() throws Exception {
    Document doc = Document.create("doc1");
    ObjectNode obj = MAPPER.createObjectNode();
    obj.put("f", 1.1f);
    doc.setField("obj", obj);

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc1\",\"obj\":{\"f\":1.1}}\n", bulkBody(config(""), doc));
  }

  @Test
  public void testBigNumbers() throws Exception {
    Document doc = Document.create("doc1");
    ObjectNode obj = MAPPER.createObjectNode();
    obj.put("dec", new BigDecimal("12345678901234567890.123456789"));
    obj.put("int", new BigInteger("123456789012345678901234567890"));
    doc.setField("obj", obj);
    doc.setField("dec", obj.get("dec"));
    doc.setField("int", obj.get("int"));

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc1\",\"obj\":{\"dec\":12345678901234567890.123456789,\"int\":123456789012345678901234567890},"
        + "\"dec\":12345678901234567890.123456789,\"int\":123456789012345678901234567890}\n", bulkBody(config(""), doc));
  }

  @Test
  public void testJsonRoundTrip() throws Exception {
    Document original = Document.create("doc1");
    populateScalars(original);
    original.setField("obj", MAPPER.readTree(NESTED_JSON));

    // after a JSON round trip byte[] is base64 text
    String expected = "{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc1\",\"str\":\"s\",\"int\":1,\"long\":1234567890123,\"double\":1.5,\"float\":1.1,"
        + "\"bool\":true,\"bytes\":\"AQID/w==\",\"multi\":[\"v1\",\"v2\"]," + NESTED_BODY + "}\n";

    assertEquals(expected, bulkBody(config(""), Document.createFromJson(original.toString())));
    assertEquals(expected,
        bulkBody(config(""), new KafkaDocument((ObjectNode) MAPPER.readTree(original.toString()))));
  }

  @Test
  public void testWhitelistAndBlacklist() throws Exception {
    Document doc = Document.create("doc1");
    populateScalars(doc);

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"str\":\"s\",\"bytes\":\"AQID/w==\"}\n",
        bulkBody(config("indexer.whitelist: [\"str\", \"bytes\", \"nullField\"]"), doc));

    assertEquals("{\"index\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc1\",\"int\":1,\"long\":1234567890123,\"double\":1.5,\"float\":1.1,\"bool\":true,"
        + "\"multi\":[\"v1\",\"v2\"]}\n",
        bulkBody(config("indexer.blacklist: [\"str\", \"bytes\", \"nullField\"]"), doc));
  }

  @Test
  public void testIdOverride() throws Exception {
    Document doc = Document.create("doc1");
    doc.setField("other_id", "other1");
    doc.setField("field1", "value1");

    // both id fields unfiltered: the override is the bulk id and replaces the id field's value
    assertEquals("{\"index\":{\"_id\":\"other1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"other1\",\"other_id\":\"other1\",\"field1\":\"value1\"}\n",
        bulkBody(config("indexer.idOverrideField: \"other_id\""), doc));

    // id filtered out: the id field is not added back
    assertEquals("{\"index\":{\"_id\":\"other1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"other_id\":\"other1\",\"field1\":\"value1\"}\n",
        bulkBody(config("indexer.idOverrideField: \"other_id\"\nindexer.blacklist: [\"id\"]"), doc));

    // id and override both filtered out
    assertEquals("{\"index\":{\"_id\":\"other1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"field1\":\"value1\"}\n",
        bulkBody(config("indexer.idOverrideField: \"other_id\"\nindexer.blacklist: [\"id\", \"other_id\"]"), doc));

    // override configured but absent from the document
    Document noOverride = Document.create("doc2");
    assertEquals("{\"index\":{\"_id\":\"doc2\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"doc2\"}\n",
        bulkBody(config("indexer.idOverrideField: \"other_id\""), noOverride));
  }

  @Test
  public void testChildren() throws Exception {
    Document parent = Document.create("parent1");
    parent.setField("parent_field", "p");
    Document child1 = Document.create("child1");
    child1.setField("child_field", "c1");
    child1.setField("child_null", (String) null);
    child1.setField("child_float", 2.2f);
    child1.setField("child_bytes", new byte[] {4, 5});
    Document grandchild = Document.create("grandchild1");
    grandchild.setField("gc", "g");
    child1.addChild(grandchild);
    Document child2 = Document.create("child2");
    child2.setField("ignored", "x");
    parent.addChild(child1);
    parent.addChild(child2);
    parent.setField("after_children", "a");

    assertEquals("{\"index\":{\"_id\":\"parent1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"parent1\",\"parent_field\":\"p\",\"after_children\":\"a\",\"kids\":["
        + "{\"id\":\"child1\",\"child_field\":\"c1\",\"child_float\":2.2,\"child_bytes\":\"BAU=\"},"
        + "{\"id\":\"child2\"}]}\n",
        bulkBody(config("opensearch.childDocumentsField: \"kids\"\nindexer.blacklist: [\"ignored\"]"), parent));

    // without childDocumentsField the children are dropped
    assertEquals("{\"index\":{\"_id\":\"parent1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"id\":\"parent1\",\"parent_field\":\"p\",\"after_children\":\"a\"}\n",
        bulkBody(config(""), parent));
  }

  @Test
  public void testUpdate() throws Exception {
    Document doc = Document.create("doc1");
    populateScalars(doc);

    assertEquals("{\"update\":{\"_id\":\"doc1\",\"_index\":\"lucille-default\"}}\n"
        + "{\"doc\":" + SCALARS_BODY + "}\n", bulkBody(config("opensearch.update: true"), doc));
  }

  // The streamed body of a JsonDocument is byte for byte the JSON of the Map body it replaces, for the same documents
  // and configurations, including values the Map path converts because they contain nulls.
  @Test
  public void testStreamedBodyMatchesMapBody() throws Exception {
    Document scalars = Document.create("doc1");
    populateScalars(scalars);
    scalars.setField("instant", Instant.parse("2024-01-02T03:04:05.678Z"));
    scalars.setField("nan", Double.NaN);

    Document nested = Document.create("doc2");
    nested.setField("obj", MAPPER.readTree(NESTED_JSON));
    ObjectNode withNulls = MAPPER.createObjectNode();
    withNulls.put("dec", new BigDecimal("12345678901234567890.123456789"));
    withNulls.put("int", new BigInteger("123456789012345678901234567890"));
    withNulls.put("f", 1.1f);
    withNulls.put("bytes", new byte[] {1, 2, 3});
    withNulls.putNull("n");
    withNulls.putArray("arr").addNull().add(2.5f).addObject().putNull("x").put("y", "z");
    nested.setField("withNulls", withNulls);
    nested.setField("topNull", (String) null);
    nested.setField("pojo", new POJONode(List.of("p", 1)));

    Document jsonDoc = Document.createFromJson("{\"id\":\"doc3\",\"big\":12345678901234567890,\"d\":1.0E-7,"
        + "\"s\":\"\\u00e9\\n\\\"\",\"a\":[[],{}],\"o\":{\"n\":null}}");

    Document parent = Document.create("doc4");
    parent.setField("kids", "replaced in place");
    parent.setField("after", "a");
    Document child = Document.create("child1");
    child.setField("c", MAPPER.readTree("{\"n\":null,\"v\":[null,1]}"));
    child.setField("ignored", "x");
    child.addChild(Document.create("grandchild1"));
    parent.addChild(child);
    parent.addChild(Document.create("child2"));

    Document nullKids = Document.create("doc5");
    nullKids.setField("kids", (String) null);
    nullKids.setField("other_id", "other5");
    nullKids.addChild(Document.create("child3"));

    List<Document> docs = List.of(scalars, nested, jsonDoc, parent, nullKids);
    List<String> configs = List.of("",
        "opensearch.childDocumentsField: \"kids\"",
        "opensearch.childDocumentsField: \"kids\"\nindexer.blacklist: [\"ignored\", \"str\", \"kids\"]",
        "indexer.whitelist: [\"id\", \"obj\", \"kids\", \"c\", \"after\"]\nopensearch.childDocumentsField: \"kids\"",
        "indexer.idOverrideField: \"other_id\"",
        "indexer.idOverrideField: \"other_id\"\nindexer.blacklist: [\"id\"]");

    JacksonJsonpMapper mapper = new JacksonJsonpMapper();
    for (String overrides : configs) {
      OpenSearchIndexer indexer = new OpenSearchIndexer(config(overrides), new TestMessenger(), "testing", mockClient);
      for (Document doc : docs) {
        String docId = doc.has("other_id") && overrides.contains("idOverrideField") ? doc.getString("other_id") : doc.getId();
        Object streamed = indexer.getUploadBody(doc, docId);
        assertTrue(streamed instanceof JsonpSerializable);
        byte[] expected = serialize(indexer.getMapBody(doc, docId), mapper);
        assertArrayEquals("config [" + overrides + "], document " + doc.getId() + ": expected "
                + new String(expected, StandardCharsets.UTF_8) + " but was "
                + new String(serialize(streamed, mapper), StandardCharsets.UTF_8),
            expected, serialize(streamed, mapper));
      }
    }
  }

  // A Document that isn't backed by a JSON tree keeps the Map body.
  @Test
  public void testNonJsonDocumentGetsMapBody() {
    Document doc = Mockito.mock(Document.class);
    when(doc.getId()).thenReturn("doc1");
    when(doc.asMap()).thenReturn(new LinkedHashMap<>(Map.of("id", "doc1", "field", "value")));

    OpenSearchIndexer indexer = new OpenSearchIndexer(config(""), new TestMessenger(), "testing", mockClient);
    Object body = indexer.getUploadBody(doc, "doc1");
    assertEquals(Map.of("id", "doc1", "field", "value"), body);
  }

  // The streamed body can only be written to a Jackson generator, and says so.
  @Test
  public void testStreamedBodyRequiresJacksonGenerator() {
    OpenSearchIndexer indexer = new OpenSearchIndexer(config(""), new TestMessenger(), "testing", mockClient);
    JsonpSerializable body = (JsonpSerializable) indexer.getUploadBody(Document.create("doc1"), "doc1");
    JsonGenerator generator = Mockito.mock(JsonGenerator.class);
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> body.serialize(generator, new JacksonJsonpMapper()));
    assertTrue(e.getMessage().contains("Jackson generator"));
  }

  private static byte[] serialize(Object value, JsonpMapper mapper) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    JsonGenerator generator = mapper.jsonProvider().createGenerator(out);
    mapper.serialize(value, generator);
    generator.close();
    return out.toByteArray();
  }

  // nullField is absent: the client's JacksonJsonpMapper leaves out null map values, at any depth, while
  // nulls inside arrays are written
  private static final String SCALARS_BODY = "{\"id\":\"doc1\",\"str\":\"s\",\"int\":1,\"long\":1234567890123,"
      + "\"double\":1.5,\"float\":1.1,\"bool\":true,\"bytes\":\"AQID/w==\",\"multi\":[\"v1\",\"v2\"]}";

  private static final String NESTED_JSON =
      "{\"a\": 1, \"b\": null, \"c\": {\"d\": [1, null, \"x\"], \"e\": null}, \"f\": 1.25}";
  private static final String NESTED_BODY = "\"obj\":{\"a\":1,\"c\":{\"d\":[1,null,\"x\"]},\"f\":1.25}";

  private static void populateScalars(Document doc) {
    doc.setField("str", "s");
    doc.setField("int", 1);
    doc.setField("long", 1234567890123L);
    doc.setField("double", 1.5);
    doc.setField("float", 1.1f);
    doc.setField("bool", true);
    doc.setField("nullField", (String) null);
    doc.setField("bytes", new byte[] {1, 2, 3, (byte) 0xff});
    doc.addToField("multi", "v1");
    doc.addToField("multi", "v2");
  }

  private static Config config(String overrides) {
    return ConfigFactory.parseString(overrides).withFallback(ConfigFactory.load("OpenSearchIndexerTest/config.conf"));
  }

  /**
   * Runs the given document through a fresh OpenSearchIndexer and returns the bulk request body, checking that
   * the document itself was not modified.
   */
  private String bulkBody(Config config, Document doc) throws Exception {
    Mockito.clearInvocations(mockClient);
    Document before = doc.deepCopy();

    TestMessenger messenger = new TestMessenger();
    OpenSearchIndexer indexer = new OpenSearchIndexer(config, messenger, "testing", mockClient);
    messenger.sendForIndexing(doc);
    indexer.run(1);

    assertEquals(before, doc);

    ArgumentCaptor<BulkRequest> captor = ArgumentCaptor.forClass(BulkRequest.class);
    verify(mockClient, times(1)).bulk(captor.capture());
    return toNdJson(captor.getValue(), new JacksonJsonpMapper());
  }

  // mirrors how the client's transport writes a bulk request body
  private static String toNdJson(NdJsonpSerializable value, JsonpMapper mapper) {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    List<Object> items = new ArrayList<>();
    collect(value, items);
    for (Object item : items) {
      JsonGenerator generator = mapper.jsonProvider().createGenerator(out);
      mapper.serialize(item, generator);
      generator.close();
      out.write('\n');
    }
    return out.toString(StandardCharsets.UTF_8);
  }

  private static void collect(NdJsonpSerializable value, List<Object> items) {
    Iterator<?> values = value._serializables();
    while (values.hasNext()) {
      Object item = values.next();
      if (item instanceof NdJsonpSerializable serializable && item != value) {
        collect(serializable, items);
      } else {
        items.add(item);
      }
    }
  }
}
