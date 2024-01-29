package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;

public class HTMLTextExtractTest {

  private StageFactory factory = StageFactory.of(HTMLTextExtract.class);

  @Test
  public void testRequiredPropertiesNotProvided() {
    assertThrows(StageException.class, () -> {
      factory.get("HTMLTextExtractTest/noFilePath.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("HTMLTextExtractTest/noDestinationFields.conf");
    });
  }

  @Test
  public void testFilePathFieldNotInDocument() throws StageException {
    Stage stage = factory.get("HTMLTextExtractTest/basic.conf");
    Document doc = Document.create("doc1");
    stage.processDocument(doc);

    assertEquals(1, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
  }

  @Test
  public void testFileNotExists() throws StageException {
    Stage stage = factory.get("HTMLTextExtractTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/HTMLTextExtractTest/does-not-exist.html");

    assertThrows(StageException.class, () -> {
      stage.processDocument(doc);
    });
  }

  @Test
  public void testOverwritesField() throws StageException {
    Stage stage = factory.get("HTMLTextExtractTest/basic.conf");
    Document doc = Document.create("doc1");
    doc.setField("destination", "something is already here");
    String path = "src/test/resources/HTMLTextExtractTest/single-match.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("new data", doc.getString("destination"));
  }

  @Test
  public void testMultipleElementsMatch() throws StageException {
    Stage stage = factory.get("HTMLTextExtractTest/basic.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/HTMLTextExtractTest/multiple-matches.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals(List.of("match 1", "match 2"), doc.getStringList("destination"));
  }

  @Test
  public void testMultipleDestinations() throws StageException {
    Stage stage = factory.get("HTMLTextExtractTest/multipleDestinations.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/HTMLTextExtractTest/two-tags.html";
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
    Stage stage = factory.get("HTMLTextExtractTest/css.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/HTMLTextExtractTest/css.html";
    doc.setField("path", path);
    stage.processDocument(doc);

    assertEquals(5, doc.getFieldNames().size());
    assertEquals("doc1", doc.getString("id"));
    assertEquals(path, doc.getString("path"));
    assertEquals("content 1", doc.getString("destination1"));
    assertEquals("content 2", doc.getString("destination2"));
    assertEquals("content 3", doc.getString("destination3"));
  }
}
