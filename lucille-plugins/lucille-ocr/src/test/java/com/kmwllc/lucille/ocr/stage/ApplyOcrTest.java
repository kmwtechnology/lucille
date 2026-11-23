package com.kmwllc.lucille.ocr.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.StageFactory;

public class ApplyOcrTest {

  StageFactory factory = StageFactory.of(ApplyOcr.class);

  @Test
  public void testMalformedConfigs() {
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/noFilePath.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/pagesNoExtractionTemps.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/pagesFieldNoExtractionTemps.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/badExtractionTemps.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/pagesNoNumbers.conf");
    });
    assertThrows(StageException.class, () -> {
      factory.get("ApplyOcrTest/noLang.conf");
    });
  }

  @Test
  public void testLang() throws StageException {
    Stage basicEng = factory.get("ApplyOcrTest/basic_eng.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/ApplyOcrTest/images/HelloWorld.png");

    basicEng.processDocument(doc);

    assertEquals("Hello, World!\n", doc.getString("dest"));
  }

  @Test
  public void testExtractAllPdf() throws StageException {
    Stage basicEng = factory.get("ApplyOcrTest/basic_eng.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/ApplyOcrTest/images/twoPage.pdf");

    basicEng.processDocument(doc);

    assertEquals(List.of("foo\n", "bar\n"), doc.getStringList("dest"));
  }

  @Test
  public void testStaticTemplates() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/staticTemplates.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/formEx1.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(6, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("name\n", doc.getString("one"));
    assertEquals("", doc.getString("two"));
    assertEquals("occupation\n", doc.getString("three"));
    assertEquals("title\n", doc.getString("four"));
  }

  @Test
  public void testDynamicTemplates() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/dynamicTemplates.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/formEx1.pdf";
    doc.setField("path", path);
    String pages = "{\"1\": \"form3\"}";
    doc.setField("pages", pages);

    stage.processDocument(doc);

    assertEquals(6, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("name\n", doc.getString("one"));
    assertEquals("", doc.getString("two"));
    assertEquals("occupation\n", doc.getString("three"));
    assertEquals(pages, doc.getString("pages"));
  }

  @Test 
  public void testDuplicateLabelAppends() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/appendTemplate.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/formEx1.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals(List.of("name\n", "", "occupation\n", "title\n"), doc.getStringList("one"));
  }

  @Test 
  public void testNonPdfFormExtraction() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/nonPDF.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/HelloWorld.png";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(3, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("Hello, World!\n", doc.getString("one"));
  }

  @Test
  public void testTemplateNotExists() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/templateNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/formEx1.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(6, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("name\n", doc.getString("one"));
    assertEquals("", doc.getString("two"));
    assertEquals("occupation\n", doc.getString("three"));
    assertEquals("title\n", doc.getString("four"));
  }

  @Test
  public void testPageNotExists() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/pageNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/formEx1.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(5, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("name\n", doc.getString("one"));
    assertEquals("", doc.getString("two"));
    assertEquals("occupation\n", doc.getString("three"));
  }

  @Test
  public void testFileDoesNotExist() throws StageException {
    Stage stage = factory.get("ApplyOcrTest/pageNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOcrTest/images/doesNotExist.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(2, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("doc1", doc.getString("id"));
  }
}
