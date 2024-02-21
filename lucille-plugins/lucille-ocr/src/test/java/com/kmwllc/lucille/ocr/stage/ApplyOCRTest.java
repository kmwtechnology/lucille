package com.kmwllc.lucille.ocr.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import java.util.List;
import org.junit.Test;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.StageFactory;
import com.kmwllc.lucille.util.StageUtils;

public class ApplyOCRTest {

  StageFactory factory = StageFactory.of(ApplyOCR.class);

  // @Test
  // public void testMalformedConfigs() {
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/noFilePath.conf");
  //   });
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/pagesNoExtractionTemps.conf");
  //   });
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/pagesFieldNoExtractionTemps.conf");
  //   });
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/badExtractionTemps.conf");
  //   });
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/pagesNoNumbers.conf");
  //   });
  //   assertThrows(StageException.class, () -> {
  //     factory.get("ApplyOCRTest/noLang.conf");
  //   });
  // }
  //
  // @Test
  // public void testLang() throws StageException {
  //   Stage basicEng = factory.get("ApplyOCRTest/basic_eng.conf");
  //   Stage basicJpn = factory.get("ApplyOCRTest/basic_jpn.conf");
  //   Document doc = Document.create("doc1");
  //   Document doc2 = Document.create("doc2");
  //   doc.setField("path", "src/test/resources/ApplyOCRTest/images/microsoft.png");
  //   doc2.setField("path", "src/test/resources/ApplyOCRTest/images/japanese.png");
  //
  //   basicEng.processDocument(doc);
  //   basicJpn.processDocument(doc2);
  //
  //   assertEquals("MICRZ2SOFT\n\nWORD\n\n", doc.getString("dest"));
  //   assertEquals(
  //       "の 兆 料 時\n\nVe White Snake Transform Time\nさ\n大 科 ト に 良 復 ギャ\n< っ\n\nValor\nSClenC6e ニニ ーー ROnin ーー ーー Revense ーー に go'。\n\nTengu\n\n圭 ・\nヒア ェ\nFuture Flving Dragon\n\n40 JaDanese Characters\n\n",
  //       doc2.getString("dest"));
  // }

  // @Test
  // public void testExtractAllPdf() throws StageException {
  //   Stage basicEng = factory.get("ApplyOCRTest/basic_eng.conf");
  //   Document doc = Document.create("doc1");
  //   doc.setField("path", "src/test/resources/ApplyOCRTest/images/twoPage.pdf");
  //
  //   basicEng.processDocument(doc);
  //
  //   assertEquals(List.of("foo\n", "bar\n"), doc.getStringList("dest"));
  // }

  // @Test
  // public void testStaticTemplates() throws StageException {
  //   Stage stage = factory.get("ApplyOCRTest/staticTemplates.conf");
  //   Document doc = Document.create("doc1");
  //   String path = "src/test/resources/ApplyOCRTest/images/formEx1.pdf";
  //   doc.setField("path", path);
  //
  //   stage.processDocument(doc);
  //
  //   assertEquals(6, doc.getFieldNames().size());
  //   assertEquals(path, doc.getString("path"));
  //   assertEquals("name\n", doc.getString("one"));
  //   assertEquals("", doc.getString("two"));
  //   assertEquals("occupation\n", doc.getString("three"));
  //   assertEquals("title\n", doc.getString("four"));
  // }

  @Test
  public void testDynamicTemplates() throws StageException {
    Stage stage = factory.get("ApplyOCRTest/dynamicTemplates.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOCRTest/images/formEx1.pdf";
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
  public void testDuplicateLabelAppends() {

  }
  @Test
  public void testTemplateNotExists() throws StageException {
    Stage stage = factory.get("ApplyOCRTest/templateNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOCRTest/images/formEx1.pdf";
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
    Stage stage = factory.get("ApplyOCRTest/pageNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOCRTest/images/formEx1.pdf";
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
    Stage stage = factory.get("ApplyOCRTest/pageNoExist.conf");
    Document doc = Document.create("doc1");
    String path = "src/test/resources/ApplyOCRTest/images/doesNotExist.pdf";
    doc.setField("path", path);

    stage.processDocument(doc);

    assertEquals(2, doc.getFieldNames().size());
    assertEquals(path, doc.getString("path"));
    assertEquals("doc1", doc.getString("id"));
  }
}
