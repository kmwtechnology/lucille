package com.kmwllc.lucille.all;

import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.stage.StageFactory;
import com.kmwllc.lucille.tika.stage.TextExtractor;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;

/**
 * Guards against pdfbox version conflicts between lucille-tika (Tika's PDF parser) and lucille-ocr when both are on the
 * classpath. A mismatch surfaces as NoSuchFieldError from Tika's PDF parser.
 */
public class PdfExtractionTest {

  @Test
  public void testTextExtractorParsesPdfOnCombinedClasspath() throws Exception {
    Stage stage = StageFactory.of(TextExtractor.class).get(Map.of(
        "textField", "text",
        "filePathField", "path"));
    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/PdfExtractionTest/tika.pdf").toAbsolutePath().toString());

    stage.processDocument(doc);

    assertTrue(doc.getString("text").contains("Hi There!"));
  }
}
