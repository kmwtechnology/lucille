package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class ApplyJSoupTest {

  private StageFactory factory = StageFactory.of(ApplyJSoup.class);

  @Test
  public void testRequiredPropertiesNotProvided() {
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/noDestinationFields.conf");
    });
  }

  @Test
  public void testOneAndOnlyOneSourceProvided() {
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/two-sources-1.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/two-sources-2.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/two-sources-3.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/zero-sources.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/three-sources.conf");
    });
  }

  @Test
  public void testDestinationFieldsMapIncorrect() throws StageException {
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/3-fields.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/no-attribute.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/no-selector.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/number-mapping.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyJSoupTest/list-mapping.conf");
    });
  }

  @Test
  public void testFilePathFieldNotInDocument() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/basic.conf");
    Document doc = Document.create("doc1");
    stage.processDocument(doc);

    assertEquals(1, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
  }

  @Test
  public void testFileNotExists() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/ApplyJSoupTest/does-not-exist.html");

    assertThrows(StageException.class, () -> {
      stage.processDocument(doc);
    });
  }

  @Test
  public void testOverwritesField() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("destination", "something is already here");
    String path = "src/test/resources/ApplyJSoupTest/single-match.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("new data", doc.getString("destination"));
  }

  @Test
  public void testMultipleElementsMatch() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/basic.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyJSoupTest/multiple-matches.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals(List.of("match 1", "match 2"), doc.getStringList("destination"));
  }

  @Test
  public void testMultipleDestinations() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/multipleDestinations.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyJSoupTest/two-tags.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(4, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("content 1", doc.getString("destination1"));
    assertEquals("content 2", doc.getString("destination2"));
  }

  @Test
  public void testBasicCssSelectors() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/css.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyJSoupTest/css.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(5, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("content 1", doc.getString("destination1"));
    assertEquals("content 2", doc.getString("destination2"));
    assertEquals("content 3", doc.getString("destination3"));
  }

  @Test
  public void testBasicCssSelectorsBytes() throws StageException, IOException {
    Stage stage = factory.get("ApplyJSoupTest/basic-bytes.conf");
    Document doc = Document.create("doc1");
    byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/ApplyJSoupTest/css.html"));
    doc.setField("bytes", bytes);
    stage.processDocument(doc);

    assertEquals(5, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(bytes, doc.getBytes("bytes"));
    assertEquals("content 1", doc.getString("destination1"));
    assertEquals("content 2", doc.getString("destination2"));
    assertEquals("content 3", doc.getString("destination3"));
  }

  @Test
  public void testExtractAttributes() throws StageException, IOException {
    Stage stage = factory.get("ApplyJSoupTest/extract-attributes.conf");
    Document doc = Document.create("doc1");
    byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/ApplyJSoupTest/page-with-attributes.html"));
    doc.setField("bytes", bytes);
    stage.processDocument(doc);

    assertEquals(4, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(bytes, doc.getBytes("bytes"));
    assertEquals("link", doc.getString("destinationText"));
    assertEquals("google.com", doc.getString("destinationAttribute"));
  }

  @Test
  public void testStringField() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/stringField.conf");
    Document doc = Document.create("doc1");
    String html = "<html><div>foo</div><a>bar</a></html>";
    doc.setField("string", html);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(html, doc.getString("string"));
    assertEquals("bar", doc.getString("destination"));
  }

  @Test 
  public void testExtractInnerHtml() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/inner-html.conf");
    Document doc = Document.create("doc1");
    String html = "<html><div>foo</div><div>bar</div></html>";
    doc.setField("string", html);
    stage.processDocument(doc);;

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(List.of("foo", "bar"), doc.getStringList("destination"));
    assertEquals(html, doc.getString("string"));
  }


  @Test 
  public void testExtractOuterHtml() throws StageException {
    Stage stage = factory.get("ApplyJSoupTest/outer-html.conf");
    Document doc = Document.create("doc1");
    String html = "<html><div>foo</div><div>bar</div></html>";
    doc.setField("string", html);
    stage.processDocument(doc);;

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(List.of("<div>\n foo\n</div>", "<div>\n bar\n</div>"), doc.getStringList("destination"));
    assertEquals(html, doc.getString("string"));
  }
}
