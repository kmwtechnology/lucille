package com.kmwllc.lucille.tika.stage;

import com.kmwllc.lucille.core.Document;

import com.kmwllc.lucille.core.Stage;

import com.kmwllc.lucille.core.StageException;

import com.kmwllc.lucille.tika.stage.StageFactory;

import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

import java.io.File;

import java.io.IOException;

import java.nio.file.Files;

import java.util.Base64;

import static org.junit.Assert.assertEquals;

import static org.junit.Assert.assertTrue;

public class TextExtractorTest {

  private StageFactory factory = StageFactory.of(TextExtractor.class);

  /**
   * Tests the TextExtractor on a config with a specified file path.
   *
   * @throws StageException
   */
  @Test
  public void testFilePath() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.txt");
    stage.processDocument(doc);
    assertEquals("Hi There!\n", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor on a config with a specified byteArray.
   *
   * @throws StageException
   */
  @Test
  public void testByteArray() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/bytearray.conf");
    Document doc = Document.create("doc1");
    File file = new File("src/test/resources/TextExtractorTest/tika.txt");
    byte[] fileContent = Files.readAllBytes(file.toPath());
    //String val = Base64.getEncoder().encodeToString(fileContent);
    doc.setField("byteArray", fileContent);
    stage.processDocument(doc);
    assertEquals("Hi There!\n", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor on a config with a Docx file.
   *
   * @throws StageException
   */
  @Test
  public void testDocx() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");
    Document doc = Document.create("doc1");
    doc.setField("filepath", "src/test/resources/TextExtractorTest/tika.docx");
    stage.processDocument(doc);
    assertEquals("Hi There!\n", doc.getString("text"));
    assertEquals("Microsoft Office Word", doc.getString("tika_extended_properties_application"));
  }

  /**
   * Tests the TextExtractor on a config with a Excel file.
   *
   * @throws StageException
   */
  @Test
  public void testExcel() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.xlsx");
    stage.processDocument(doc);
    assertEquals("Sheet1\n" +
        "\tHi There!\n" +
        "\n" +
        "\n", doc.getString("text"));
    assertEquals("Microsoft Macintosh Excel", doc.getString("tika_extended_properties_application"));
  }

  /**
   * Tests the TextExtractor with a custom Tika config.
   *
   * @throws StageException
   */
  @Test
  public void testCustomTikaConfig() throws StageException {
    Stage stage = factory.get("TextExtractorTest/tika-config.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.txt");
    stage.processDocument(doc);
    assertEquals("Hi There!\n", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor with a custom Tika config.
   *
   * @throws StageException
   */
  @Test
  public void testCustomTikaConfig2() throws StageException {
    Stage stage = factory.get("TextExtractorTest/tika-config2.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.pdf");
    stage.processDocument(doc);
    // verify that the open parser is what is used on pdfs
    assertTrue(doc.getStringList("tika_x_tika_parsed_by").contains("org.apache.tika.parser.EmptyParser"));
  }
}