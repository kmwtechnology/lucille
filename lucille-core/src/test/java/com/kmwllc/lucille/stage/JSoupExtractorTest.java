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

public class JSoupExtractorTest {

  private StageFactory factory = StageFactory.of(JSoupExtractor.class);

  @Test
  public void testRequiredPropertiesNotProvided() {
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/noDestinationFields.conf");
    });
  }

  @Test
  public void testTwoSourcesProvided() {
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/two-sources.conf");
    });
  }

  @Test
  public void testDestinatinFieldsMapIncorrect() throws StageException {
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/3-fields.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/no-attribute.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/no-selector.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/number-mapping.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("JSoupExtractorTest/list-mapping.conf");
    });
  }

  @Test
  public void testFilePathFieldNotInDocument() throws StageException {
    Stage stage = factory.get("JSoupExtractorTest/basic.conf");
    Document doc = Document.create("doc1");
    stage.processDocument(doc);

    assertEquals(1, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
  }

  @Test
  public void testFileNotExists() throws StageException {
    Stage stage = factory.get("JSoupExtractorTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/JSoupExtractorTest/does-not-exist.html");

    assertThrows(StageException.class, () -> {
      stage.processDocument(doc);
    });
  }

  @Test
  public void testOverwritesField() throws StageException {
    Stage stage = factory.get("JSoupExtractorTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("destination", "something is already here");
    String path = "src/test/resources/JSoupExtractorTest/single-match.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("new data", doc.getString("destination"));
  }

  @Test
  public void testMultipleElementsMatch() throws StageException {
    Stage stage = factory.get("JSoupExtractorTest/basic.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/JSoupExtractorTest/multiple-matches.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals(List.of("match 1", "match 2"), doc.getStringList("destination"));
  }

  @Test
  public void testMultipleDestinations() throws StageException {
    Stage stage = factory.get("JSoupExtractorTest/multipleDestinations.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/JSoupExtractorTest/two-tags.html";
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
    Stage stage = factory.get("JSoupExtractorTest/css.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/JSoupExtractorTest/css.html";
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
    Stage stage = factory.get("JSoupExtractorTest/basic-bytes.conf");
    Document doc = Document.create("doc1");
    byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/JSoupExtractorTest/css.html"));
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
    Stage stage = factory.get("JSoupExtractorTest/extract-attributes.conf");
    Document doc = Document.create("doc1");
    byte[] bytes = Files.readAllBytes(Paths.get("src/test/resources/JSoupExtractorTest/page-with-attributes.html"));
    doc.setField("bytes", bytes);
    stage.processDocument(doc);

    assertEquals(4, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(bytes, doc.getBytes("bytes"));
    assertEquals("link", doc.getString("destinationText"));
    assertEquals("google.com", doc.getString("destinationAttribute"));
  }
}
