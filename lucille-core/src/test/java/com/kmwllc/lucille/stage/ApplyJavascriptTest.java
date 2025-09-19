package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;
import static org.junit.Assert.assertThrows;

public class ApplyJavascriptTest {

  private final StageFactory factory = StageFactory.of(ApplyJavascript.class);

  private Stage stageWithInlineScript(String js) throws StageException {
    Config conf = ConfigFactory.empty()
        .withValue("script", ConfigValueFactory.fromAnyRef(js));

    return factory.get(conf);
  }

  /**
   * Simple external script.
   *
   * if (doc.a != null && doc.b != null) {
   *   doc.sum = doc.a + doc.b;
   * }
   */
  @Test
  public void testValidExternalScript() throws StageException {
    Stage stage = factory.get("ApplyJavascriptTest/valid.conf");

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
    Stage stage = stageWithInlineScript("""
     throw new Error("Failure");
    """);

    Document doc = Document.create("d");

    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  @Test
  public void testCompileError() throws StageException {
    Stage stage = stageWithInlineScript("""
      function () {
        doc.x = 1;
      }
    """);

    Document doc = Document.create("bad");

    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  @Test
  public void testBadScriptPath() {
    assertThrows(StageException.class,
        // Non-existent script path
        () -> factory.get("ApplyJavascriptTest/badPath.conf"));
  }

  @Test
  public void testPropertyCopy() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.f1 = doc.f2;
    """);

    Document d = Document.create("d");
    d.setField("f2", "hello");
    stage.processDocument(d);

    assertEquals("hello", d.getString("f1"));
  }

  @Test
  public void testNumbersIntAndDouble() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.count = doc.count + 1;
      doc.pi = doc.pi + 0.14;
    """);

    Document d = Document.create("d");
    d.setField("count", 1);
    d.setField("pi", 3.0);
    stage.processDocument(d);

    assertEquals(Integer.valueOf(2), d.getInt("count"));
    assertEquals(3.14, d.getDouble("pi"), 1e-9);
  }

  @Test
  public void testSetArrayFromJs() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.tags = ["a", "b", "c"];
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals(Arrays.asList("a", "b", "c"), d.getStringList("tags"));
  }

  @Test
  public void testCopyMultiValuedField() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.tags2 = doc.tags;
    """);

    Document d = Document.create("d");
    d.update("tags", UpdateMode.OVERWRITE, "x", "y");
    stage.processDocument(d);

    assertEquals(Arrays.asList("x", "y"), d.getStringList("tags2"));
  }

  @Test
  public void testMissingFieldReadsAsNull() throws StageException {
    Stage stage = stageWithInlineScript("""
      if (doc.missing == null) {
        doc.present = "ok";
      }
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals("ok", d.getString("present"));
  }

  @Test
  public void testAssignNullStoresJsonNull() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.f = null;
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
    Stage stage = stageWithInlineScript("""
      delete doc.gone;
    """);

    Document d = Document.create("d");
    d.setField("gone", "x");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testDeleteMissingFields() throws StageException {
    Stage stage = stageWithInlineScript("""
      delete doc.gone;
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testRawDocSetFieldWritesToDoc() throws StageException {
    Stage stage = stageWithInlineScript("""
      rawDoc.setField("x", "ok");
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals("ok", d.getString("x"));
  }

  @Test
  public void testRawDocUpdateOverwriteMultivalued() throws StageException {
    Stage stage = stageWithInlineScript("""
    var UpdateMode = Java.type('com.kmwllc.lucille.core.UpdateMode');
      rawDoc.update("tags", UpdateMode.OVERWRITE, "a", "b", "c");
    """);

    Document d = Document.create("d");
    stage.processDocument(d);

    assertEquals(Arrays.asList("a","b","c"), d.getStringList("tags"));
  }

  @Test
  public void testRawDocRemoveField() throws StageException {
    Stage stage = stageWithInlineScript("""
      rawDoc.removeField("gone");
    """);

    Document d = Document.create("d");
    d.setField("gone", "x");
    stage.processDocument(d);

    assertFalse(d.has("gone"));
  }

  @Test
  public void testRawDocRemoveChildren() throws StageException {
    Stage stage = stageWithInlineScript("""
      rawDoc.removeChildren();
    """);

    Document d = Document.create("parent");
    d.addChild(Document.create("child1"));
    d.addChild(Document.create("child2"));

    assertTrue(d.hasChildren());
    stage.processDocument(d);
    assertFalse(d.hasChildren());
  }

  @Test
  public void testNestedWriteWithoutParent() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.a.b = 100; 
    """); // a does not exist

    Document d = Document.create("d");

    assertThrows(StageException.class, () -> stage.processDocument(d));
  }

  @Test
  public void testNestedWriteWithParentInitialization() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.a = doc.a || {}; doc.a.b = 100;
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
  public void testAssignNestedNullStoresJsonNull() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.meta = doc.meta || {}; doc.meta.flag = null;
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
    Stage stage = stageWithInlineScript("""
      delete doc.a.b;
    """);

    Document d = Document.create("d");
    d.setNestedJson("a.b", IntNode.valueOf(5));
    d.setNestedJson("a.c", TextNode.valueOf("keep"));

    stage.processDocument(d);

    assertNull(d.getNestedJson("a.b"));
    assertNotNull(d.getNestedJson("a.c"));
    assertEquals("keep", d.getNestedJson("a.c").asText());
    assertTrue(d.getJson("a").isObject());
  }

  @Test
  public void testDeleteNestedArrayIndex() throws StageException {
    Stage stage = stageWithInlineScript("""
      delete doc.a.list[1];
    """);

    Document d = Document.create("d");
    ObjectMapper m = new ObjectMapper();
    d.setNestedJson("a.list", m.createArrayNode().add(10).add(20).add(30));

    stage.processDocument(d);

    JsonNode list = d.getNestedJson("a.list");
    assertNotNull(list);
    assertTrue(list.isArray());
    assertEquals(2, list.size());
    assertEquals(10, list.get(0).asInt());
    assertEquals(30, list.get(1).asInt());
  }

  @Test
  public void testSetNestedArrayFromJs() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.payload = doc.payload || {}; doc.payload.items = [\"a\", \"b\", \"c\"];
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
  public void testSetNestedObjectFromJs() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.a = doc.a || {}; doc.a.meta = { build: 42, ok: true, tags: [\"x\", \"y\"] };
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
    Stage stage = stageWithInlineScript("""
      delete doc.meta.missing;
    """);

    Document d = Document.create("d");
    d.setNestedJson("meta.x", TextNode.valueOf("existing"));

    stage.processDocument(d);

    assertEquals("existing", d.getNestedJson("meta.x").asText());
    assertNull(d.getNestedJson("meta.missing"));
  }

  @Test
  public void testCreateNestedPath() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.val1 = doc.tags[1];
      doc.a = {"b": {"c": {"d": doc.val1}}};      
      delete doc.val1;      
    """);

    Document d = Document.create("doc1");
    d.update("tags", UpdateMode.OVERWRITE, "x", "y", "z");
    stage.processDocument(d);

    assertEquals("y", d.getNestedJson("a.b.c.d").asText());
  }

  @Test
  public void testCreateNestedPath2() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.a = {"b": [{},{},{},{},{"c":[{},{},{},{"d":{"e":{"f":[0,1,2,3,4,5,6,7,8,9,10] }}}]}]}; 
      doc.a.b[4].c[3].d.e.f[10] = 200;
    """);

    Document d = Document.create("doc1");
    stage.processDocument(d);

    assertEquals(200, d.getNestedJson("a.b[4].c[3].d.e.f[10]").intValue());
  }

  @Test
  public void testCreateNestedArrays() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.a = [10,20,[30,40,[50,60,[70]]]]; 
    """);

    Document d = Document.create("doc1");
    stage.processDocument(d);
    assertEquals(70, d.getNestedJson("a[2][2][2][0]").intValue());

    Stage stage2 = stageWithInlineScript("""
      doc.a[2][2][2][0] = 200;
    """);
    stage2.processDocument(d);
    assertEquals(200, d.getNestedJson("a[2][2][2][0]").intValue());
  }


  // This convenience implementation for one line nested assignment is currently not implemented.
  @Test
  public void testCreateNestedPathConvenientSyntax() throws StageException {
    Stage stage = stageWithInlineScript("""
      doc.val1 = doc.tags[1];
      doc.a.b.c.d = doc.val1;
      delete doc.val1;      
    """); // parents not auto created

    Document d = Document.create("doc1");
    d.update("tags", UpdateMode.OVERWRITE, "x", "y", "z");

    assertThrows(StageException.class, () -> stage.processDocument(d));
    assertNull(d.getNestedJson("a.b.c.d"));
  }

}