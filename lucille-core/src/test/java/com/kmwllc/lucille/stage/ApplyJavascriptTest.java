package com.kmwllc.lucille.stage;

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

// TODO: Add tests for raw doc
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
}