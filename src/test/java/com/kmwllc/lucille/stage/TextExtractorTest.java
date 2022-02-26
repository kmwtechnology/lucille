package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

import static org.junit.Assert.assertEquals;

public class TextExtractorTest {

  private StageFactory factory = StageFactory.of(TextExtractor.class);

  /**
   * Tests the TextExtractor on a config with a specified file path.
   * @throws StageException
   */
  @Test
  public void testFilePath() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");

    Document doc = new Document("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.txt");
    stage.processDocument(doc);

    System.out.println(doc.toString());

    assertEquals("Hi There!\n", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor on a config with a specified byteArray.
   * @throws StageException
   */
  @Test
  public void testByteArray() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/bytearray.conf");

    Document doc = new Document("doc1");

    File file = new File("src/test/resources/TextExtractorTest/tika.txt");
    byte[] fileContent = Files.readAllBytes(file.toPath());
    String val =  Base64.getEncoder().encodeToString(fileContent);

    doc.setField("byteArray", val);
    stage.processDocument(doc);

    System.out.println(doc.toString());

    assertEquals("Hi There!\n", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor on a config with a Docx file.
   * @throws StageException
   */
  @Test
  public void testDocx() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");

    Document doc = new Document("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.docx");
    stage.processDocument(doc);

    System.out.println(doc.toString());

    assertEquals("Hi There!\n", doc.getString("text"));
    assertEquals("Microsoft Office Word", doc.getString("tika_extended_properties_application"));
  }

  /**
   * Tests the TextExtractor on a config with a Excel file.
   * @throws StageException
   */
  @Test
  public void testExcel() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");

    Document doc = new Document("doc1");
    doc.setField("path", "src/test/resources/TextExtractorTest/tika.xlsx");
    stage.processDocument(doc);

    System.out.println(doc.toString());

    assertEquals("Sheet1\n" +
      "\tHi There!\n" +
      "\n" +
      "\n", doc.getString("text"));
    assertEquals("Microsoft Macintosh Excel", doc.getString("tika_extended_properties_application"));
  }
}
