package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.After;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class EmbeddedPythonStageTest {

  private final StageFactory factory = StageFactory.of(EmbeddedPythonStage.class);

  private Stage stageWithInline(String py) throws StageException {
    Config conf = ConfigFactory.empty().withValue("script", ConfigValueFactory.fromAnyRef(py));
    return factory.get(conf);
  }

  private Stage stage;

  @After
  public void tearDown() throws Exception {
    if (stage != null) {
      stage.stop();
      stage = null;
    }
  }

  @Test
  public void testValidExternalScript() throws StageException {
    stage = factory.get("EmbeddedPythonStageTest/valid.conf");

    Document d1 = Document.create("d1");
    d1.setField("a", 2);
    d1.setField("b", 3);
    stage.processDocument(d1);
    assertEquals(Integer.valueOf(5), d1.getInt("sum"));

    Document d2 = Document.create("d2");
    d2.setField("a", 100);
    d2.setField("b", 250);
    stage.processDocument(d2);
    assertEquals(Integer.valueOf(350), d2.getInt("sum"));
  }

  @Test
  public void testRuntimeError() throws StageException {
    stage = stageWithInline("""
    raise RuntimeError('Failure')
    """);
    Document doc = Document.create("d");
    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  @Test
  public void testCompileError() throws StageException {
    stage = stageWithInline("""
    def bad(:
    pass
    """);
    Document doc = Document.create("bad");
    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  @Test
  public void testBadScriptPath() {
    assertThrows(StageException.class,
        // Non-existent script path
        () -> factory.get("EmbeddedPythonStageTest/badPath.conf"));
  }

  @Test
  public void testPropertyCopy() throws StageException {
    stage = stageWithInline("""
    doc.f1 = doc.f2
    """);

    Document d = Document.create("d");
    d.setField("f2", "hello");
    stage.processDocument(d);

    assertEquals("hello", d.getString("f1"));
  }

  @Test
  public void testNumbersIntAndDouble() throws StageException {
    stage = stageWithInline("""
    doc.count = doc.count + 1
    doc.pi = doc.pi + 0.14
    """);

    Document d = Document.create("d");
    d.setField("count", 1);
    d.setField("pi", 3.0);
    stage.processDocument(d);

    assertEquals(Integer.valueOf(2), d.getInt("count"));
    assertEquals(3.14, d.getDouble("pi"), 1e-9);
  }

  @Test
  public void testSetArrayFromPython() throws StageException {
    stage = stageWithInline("""
    doc.tags = ['a', 'b', 'c']
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals(Arrays.asList("a", "b", "c"), d.getStringList("tags"));
  }

  @Test
  public void testCopyMultiValuedField() throws StageException {
    stage = stageWithInline("""
    doc.tags2 = doc.tags
    """);

    Document d = Document.create("d");
    d.update("tags", UpdateMode.OVERWRITE, "x", "y");
    stage.processDocument(d);

    assertEquals(Arrays.asList("x", "y"), d.getStringList("tags2"));
  }

  @Test
  public void testMissingFieldReadsAsNone() throws StageException {
    stage = stageWithInline("""
    if getattr(doc, 'missing', None) is None:
        doc.present = 'ok'
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals("ok", d.getString("present"));
  }

  @Test
  public void testAssignNoneStoresJsonNull() throws StageException {
    stage = stageWithInline("""
    doc.f = None
    """);

    Document d = Document.create("d");
    d.setField("f", "before");
    stage.processDocument(d);

    assertTrue(d.has("f"));
    assertNotNull(d.getJson("f"));
    assertTrue(d.getJson("f").isNull());
  }

  @Test
  public void testDeleteRemoveFields() throws StageException {
    stage = stageWithInline("""
    del doc.gone
    """);

    Document d = Document.create("d");
    d.setField("gone", "x");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testDeleteMissingFields() throws StageException {
    stage = stageWithInline("""
    try:
        del doc.gone
    except AttributeError:
        pass
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testRawDocSetFieldWritesToDoc() throws StageException {
    stage = stageWithInline("""
    rawDoc.setField('x', 'ok')
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals("ok", d.getString("x"));
  }

  @Test
  public void testRawDocUpdateOverwriteMultivalued() throws StageException {
    stage = stageWithInline("""
    from java import type as jtype
    UpdateMode = jtype('com.kmwllc.lucille.core.UpdateMode')
    rawDoc.update('tags', UpdateMode.OVERWRITE, 'a', 'b', 'c')
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals(Arrays.asList("a", "b", "c"), d.getStringList("tags"));
  }

  @Test
  public void testRawDocRemoveField() throws StageException {
    stage = stageWithInline("""
    rawDoc.removeField('gone')
    """);

    Document d = Document.create("d");
    d.setField("gone", "x");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testRawDocRemoveChildren() throws StageException {
    stage = stageWithInline("""
    rawDoc.removeChildren()
    """);

    Document d = Document.create("parent");
    d.addChild(Document.create("child1"));
    d.addChild(Document.create("child2"));

    assertTrue(d.hasChildren());
    stage.processDocument(d);
    assertFalse(d.hasChildren());
  }

  @Test
  public void testNestedWriteWithoutParentRaises() throws StageException {
    stage = stageWithInline("""
    doc.a.b = 100
    """); // a does not exist

    Document d = Document.create("d");
    assertThrows(StageException.class, () -> stage.processDocument(d));
  }

  @Test
  public void testNestedWriteWithParentInitialization() throws StageException {
    stage = stageWithInline("""
    doc.a = {}
    doc.a.b = 100
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode n = d.getNestedJson("a.b");
    assertNotNull(n);
    assertTrue(n.isNumber());
    assertEquals(100, n.asInt());
    assertTrue(d.getJson("a").isObject());
  }

  @Test
  public void testAssignNestedNoneStoresJsonNull() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'meta'):
        doc.meta = {}
    doc.meta.flag = None
    """);

    Document d = Document.create("d");
    d.setNestedJson("meta.x", TextNode.valueOf("existing"));

    stage.processDocument(d);

    JsonNode flag = d.getNestedJson("meta.flag");
    assertNotNull(flag);
    assertTrue(flag.isNull());
    assertEquals("existing", d.getNestedJson("meta.x").asText());
  }

  @Test
  public void testDeleteNestedFieldKeepsSiblings() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'a'):
        doc.a = {}
    doc.a.b = 5
    doc.a.c = 'keep'
    del doc.a.b
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertNull(d.getNestedJson("a.b"));
    assertNotNull(d.getNestedJson("a.c"));
    assertEquals("keep", d.getNestedJson("a.c").asText());
    assertTrue(d.getJson("a").isObject());
  }

  @Test
  public void testDeleteNestedArrayIndex() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'a'):
        doc.a = {}
    doc.a.list = [10, 20, 30]
    del doc.a.list[1]
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode list = d.getNestedJson("a.list");
    assertNotNull(list);
    assertTrue(list.isArray());
    assertEquals(2, list.size());
    assertEquals(10, list.get(0).asInt());
    assertEquals(30, list.get(1).asInt());
  }

  @Test
  public void testSetNestedArrayFromPython() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'payload'):
        doc.payload = {}
    doc.payload.items = ['a', 'b', 'c']
    """);
    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode arr = d.getNestedJson("payload.items");
    assertNotNull(arr);
    assertTrue(arr.isArray());
    assertEquals(3, arr.size());
    assertEquals("a", arr.get(0).asText());
    assertEquals("b", arr.get(1).asText());
    assertEquals("c", arr.get(2).asText());
  }

  @Test
  public void testSetNestedObjectFromPython() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'a'):
        doc.a = {}
    doc.a.meta = { 'build': 42, 'ok': True, 'tags': ['x','y'] }
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode meta = d.getNestedJson("a.meta");
    assertNotNull(meta);
    assertTrue(meta.isObject());
    assertEquals(42, meta.get("build").asInt());
    assertTrue(meta.get("ok").asBoolean());
    assertEquals("x", meta.get("tags").get(0).asText());
    assertEquals("y", meta.get("tags").get(1).asText());
  }

  @Test
  public void testDeleteNestedMissingField() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'meta'):
        doc.meta = {}
    doc.meta.x = 'existing'
    try:
        del doc.meta.missing
    except AttributeError:
        pass
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals("existing", d.getNestedJson("meta.x").asText());
    assertNull(d.getNestedJson("meta.missing"));
  }

  @Test
  public void testCreateNestedPath() throws StageException {
    stage = stageWithInline("""
    doc.val1 = ['x','y','z'][1]
    doc.a = {}
    doc.a.b = {}
    doc.a.b.c = {}
    doc.a.b.c.d = doc.val1
    del doc.val1
    """);

    Document d = Document.create("doc1");
    stage.processDocument(d);

    assertEquals("y", d.getNestedJson("a.b.c.d").asText());
  }

  @Test
  public void testCreateNestedPath2() throws StageException {
    stage = stageWithInline("""
    if not hasattr(doc, 'a'):
        doc.a = {}
    doc.a.b = [{},{},{},{},{'c':[{}, {}, {}, {'d':{'e':{'f':[0,1,2,3,4,5,6,7,8,9,10]}}}]}]
    doc.a.b[4].c[3].d.e.f[10] = 200
    """);

    Document d = Document.create("doc1");
    stage.processDocument(d);

    assertEquals(200, d.getNestedJson("a.b[4].c[3].d.e.f[10]").intValue());
  }

  @Test
  public void testCreateNestedArrays() throws StageException {
    Stage stage1 = stageWithInline("""
    doc.a = [10,20,[30,40,[50,60,[70]]]]
    """);

    Document d = Document.create("doc1");
    try {
      stage1.processDocument(d);
    } finally {
      stage1.stop();
    }
    assertEquals(70, d.getNestedJson("a[2][2][2][0]").intValue());

    Stage stage2 = stageWithInline(
        "doc.a[2][2][2][0] = 200\n"
    );
    try {
      stage2.processDocument(d);
    } finally {
      stage2.stop();
    }
    assertEquals(200, d.getNestedJson("a[2][2][2][0]").intValue());
  }

  @Test
  public void testCreateNestedPathConvenientSyntax() throws StageException {
    stage = stageWithInline("""
    doc.val1 = ['x','y','z'][1]
    doc.a.b.c.d = doc.val1
    del doc.val1
    """); // parents not auto created

    Document d = Document.create("doc1");
    assertThrows(StageException.class, () -> stage.processDocument(d));
    assertNull(d.getNestedJson("a.b.c.d"));
  }

  @Test
  public void testDictionaryGetAndCopy() throws StageException {
    stage = stageWithInline("""
    doc["copy"] = doc["field1"]
    """);

    Document d = Document.create("d");
    d.setField("field1", 42);
    stage.processDocument(d);

    assertEquals(42, d.getInt("copy").intValue());
  }

  @Test
  public void testDictionarySetRootField() throws StageException {
    stage = stageWithInline("""
    doc["field1"] = 123
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals(123, d.getInt("field1").intValue());
  }

  @Test
  public void testDictionaryOverwriteRootField() throws StageException {
    stage = stageWithInline("""
    doc["field1"] = 456
    """);

    Document d = Document.create("d");
    d.setField("field1", 123);
    stage.processDocument(d);

    assertEquals(456, d.getInt("field1").intValue());
  }

  @Test
  public void testDictionarySetNestedObjectAtRoot() throws StageException {
    stage = stageWithInline("""
    doc["nested"] = {"a": 1, "b": {"c": 2}}
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode nested = d.getJson("nested");
    assertNotNull(nested);
    assertTrue(nested.isObject());
    assertEquals(1, nested.get("a").asInt());
    assertTrue(nested.get("b").isObject());
    assertEquals(2, nested.get("b").get("c").asInt());
  }

  @Test
  public void testDictionaryMutatePreExistingNestedObject() throws StageException {
    stage = stageWithInline("""
    doc["nested"]["b"]["c"] = 99
    """);

    Document d = Document.create("d");
    ObjectNode nested = JsonNodeFactory.instance.objectNode();
    nested.put("a", 1);
    ObjectNode b = JsonNodeFactory.instance.objectNode();
    b.put("c", 2);
    nested.set("b", b);
    d.setField("nested", nested);

    stage.processDocument(d);

    JsonNode after = d.getJson("nested");
    assertNotNull(after);
    assertEquals(99, after.get("b").get("c").asInt());
  }

  @Test
  public void testDictionarySetArrayAndMutate() throws StageException {
    stage = stageWithInline("""
    doc["arr"] = [1, 2, 3]
    doc["arr"][0] = 42
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    JsonNode arr = d.getJson("arr");
    assertNotNull(arr);
    assertTrue(arr.isArray());
    assertEquals(3, arr.size());
    assertEquals(42, arr.get(0).asInt());
    assertEquals(2, arr.get(1).asInt());
    assertEquals(3, arr.get(2).asInt());
  }

  @Test
  public void testDictionaryDeleteRootField() throws StageException {
    stage = stageWithInline("""
    doc["field1"] = 123
    del doc["field1"]
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertFalse(d.has("field1"));
  }

  @Test
  public void testDictionaryKeysIteration() throws StageException {
    stage = stageWithInline("""
    keys = sorted(list(doc.keys()))
    doc["collected_keys"] = keys
    """);

    Document d = Document.create("d");
    d.setField("a", 1);
    d.setField("b", 2);
    stage.processDocument(d);

    JsonNode keys = d.getJson("collected_keys");
    assertNotNull(keys);
    assertTrue(keys.isArray());
    assertEquals(3, keys.size());
    assertEquals("a", keys.get(0).asText());
    assertEquals("b", keys.get(1).asText());
  }

  @Test
  public void testDictionaryItemsIterationEcho() throws StageException {
    stage = stageWithInline("""
    items = {k: v for (k, v) in doc.items()}
    doc["echo_items"] = items
    """);

    Document d = Document.create("d");
    d.setField("x", 10);
    d.setField("y", 20);

    stage.processDocument(d);

    JsonNode echo = d.getJson("echo_items");
    assertNotNull(echo);
    assertTrue(echo.isObject());
    assertEquals(10, echo.get("x").asInt());
    assertEquals(20, echo.get("y").asInt());
  }
}
