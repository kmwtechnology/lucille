package com.kmwllc.lucille.tika.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.StageFactory;
import com.kmwllc.lucille.util.DefaultFileContentFetcher;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class TextExtractorTest {

  private final StageFactory factory = StageFactory.of(TextExtractor.class);

  /**
   * Tests the TextExtractor on a config with a specified file path.
   *
   * @throws StageException
   */
  @Test
  public void testFilePath() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

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
    doc.setField("byte_array", fileContent);
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

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.docx").toAbsolutePath().toString());

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

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.xlsx").toAbsolutePath().toString());

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

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

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

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.pdf").toAbsolutePath().toString());

    stage.processDocument(doc);
    // verify that the open parser is what is used on pdfs
    assertTrue(doc.getStringList("mtdata_x_tika_parsed_by").contains("org.apache.tika.parser.EmptyParser"));
  }

  /**
   * Tests the content type of the TextExtractor with custom config on pdf
   *
   * @throws StageException
   */
  @Test
  public void testCustomTikaConfig2ContentType() throws StageException {
    Stage stage = factory.get("TextExtractorTest/tika-config2.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.pdf").toAbsolutePath().toString());

    stage.processDocument(doc);
    // verify that the open parser is what is used on pdfs
    assertTrue(doc.getStringList("mtdata_content_type").contains("application/pdf"));
  }

  /**
   * Tests the content type of the TextExtractor with various documents
   *
   * @throws StageException
   */
  @Test
  public void testTikaContentType() throws StageException {
    Stage stage = factory.get("TextExtractorTest/filepath.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.xlsx").toAbsolutePath().toString());
    stage.processDocument(doc1);
    assertTrue(
        doc1.getStringList("tika_content_type").contains("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"));

    Document doc2 = Document.create("doc2");
    doc2.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.docx").toAbsolutePath().toString());
    stage.processDocument(doc2);
    assertTrue(doc2.getStringList("tika_content_type")
        .contains("application/vnd.openxmlformats-officedocument.wordprocessingml.document"));

    Document doc3 = Document.create("doc3");
    doc3.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());
    stage.processDocument(doc3);
    assertTrue(doc3.getStringList("tika_content_type").contains("text/plain; charset=ISO-8859-1"));
  }

  /**
   * Tests the TextExtractor whitelist functionality
   *
   * @throws StageException
   */
  @Test
  public void testWhiteList() throws StageException {
    Stage stage = factory.get("TextExtractorTest/whitelist.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);
    assertEquals("text/plain; charset=ISO-8859-1", doc.getStringList("tika_content_type").get(0));
    assertThrows(NullPointerException.class, () -> doc.getStringList("content_encoding").get(0));
    assertThrows(NullPointerException.class, () -> doc.getStringList("tika_x_tika_parsed_by").get(0));
  }

  /**
   * Tests the TextExtractor blacklist functionality
   *
   * @throws StageException
   */
  @Test
  public void testBlackList() throws StageException {
    Stage stage = factory.get("TextExtractorTest/blacklist.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);
    assertEquals("text/plain; charset=ISO-8859-1", doc.getStringList("tika_content_type").get(0));
    assertEquals("org.apache.tika.parser.DefaultParser", doc.getStringList("tika_x_tika_parsed_by").get(0));
    assertThrows(NullPointerException.class, () -> doc.getStringList("content_encoding").get(0));
  }

  /**
   * Tests the TextExtractor size limit functionality
   *
   * @throws StageException
   */
  @Test
  public void testSizeLimit() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/sizelimit.conf");
    Document doc = Document.create("doc1");
    File file = new File("src/test/resources/TextExtractorTest/tika.txt");
    byte[] fileContent = Files.readAllBytes(file.toPath());
    doc.setField("byte_array", fileContent);
    stage.processDocument(doc);
    assertEquals("Hi ", doc.getString("text"));
  }

  /**
   * Tests the TextExtractor closes inputStream after Document is processed
   *
   * @throws StageException
   */
  @Test
  public void testInputStreamClose() throws Exception {

    // mock fileFetcher
    FileContentFetcher mockFetcher = mock(FileContentFetcher.class);
    InputStream inputStream = spy(new ByteArrayInputStream("Hello World".getBytes()));

    try (MockedConstruction<DefaultFileContentFetcher> mockedConstruction = mockConstruction(DefaultFileContentFetcher.class,
        (mock, context) -> {
          when(mock.getInputStream(anyString())).thenReturn(inputStream);
        })) {
      Config config = ConfigFactory.parseString("fetcherClass = \"com.kmwllc.lucille.util.DefaultFileContentFetcher\"\n" +
          "filePathField = \"path\"\n" +
          "textField = \"text\"");
      TextExtractor stage = new TextExtractor(config);
      stage.start();

      // set up document
      Document doc = Document.create("doc1");
      doc.setField("path", "some-path");

      // go through the process of processing the document.
      stage.processDocument(doc);

      // verify that close method is called on the inputStream
      verify(inputStream, times(1)).close();
    }

  }


  /**
   * Tests the TextExtractor getting inputStream from file method
   */
  @Test
  public void testGetFileInputStreamError() throws StageException, IOException {
    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/getFileInputStream.conf");

    // setting document to have a non-existent path
    Document doc = Document.create("doc1");
    doc.addToField("path", Paths.get("src/test/resources/TextExtractorTest/nonExistentFile").toAbsolutePath().toString());

    // parsing through the document would then be passed through and not parsed with log statements
    stage.processDocument(doc);
    Assert.assertNull(doc.getString("text"));
  }

  @Test
  public void testSingleAndMultiValueField() throws Exception {
    Stage stage = factory.get("TextExtractorTest/tika-config.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    // test that field that is added once is single valued while field that is added multiple times is multiValued in a list
    Map<String, Object> fields = doc.asMap();

    assertEquals("Hi There!\n", fields.get("text"));
    assertEquals(List.of("org.apache.tika.parser.CompositeParser",
        "org.apache.tika.parser.DefaultParser",
        "org.apache.tika.parser.csv.TextAndCSVParser"), fields.get("tika_x_tika_parsed_by"));
    assertEquals("ISO-8859-1", fields.get("tika_content_encoding"));
    assertEquals("text/plain; charset=ISO-8859-1", fields.get("tika_content_type"));
  }

  @Test
  public void testExtractionWithURI() throws Exception {
    Stage stage = factory.get("TextExtractorTest/tika-config.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toUri().toString());
    stage.processDocument(doc);

    Map<String, Object> fields = doc.asMap();
    assertEquals("Hi There!\n", fields.get("text"));
    assertEquals(List.of("org.apache.tika.parser.CompositeParser",
        "org.apache.tika.parser.DefaultParser",
        "org.apache.tika.parser.csv.TextAndCSVParser"), fields.get("tika_x_tika_parsed_by"));
    assertEquals("ISO-8859-1", fields.get("tika_content_encoding"));
    assertEquals("text/plain; charset=ISO-8859-1", fields.get("tika_content_type"));
  }

  public static class InterruptTrackingParser extends DefaultParser {

    private static AtomicBoolean interrupted = new AtomicBoolean(false);

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
      return Collections.singleton(MediaType.TEXT_PLAIN);
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
        throws TikaException, IOException, SAXException {
      try {
        Thread.sleep(200);
        super.parse(stream, handler, metadata, context);
      } catch (InterruptedException e) {
        interrupted.set(true);
      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    InterruptTrackingParser.interrupted.set(false);

    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/timeout.conf");

    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    // Give it a bit of time for the async task to be interrupted and set the flag
    Thread.sleep(200);

    // If timeout works, it should have returned within much less than 1000ms
    // and interrupted should be true (if we interrupt the thread)
    assertTrue("Parser should have been interrupted", InterruptTrackingParser.interrupted.get());
    // Document should not have text (or at least not from the parser)
    assertEquals("Document should have empty text.", "", doc.getString("text"));

    stage.stop();

    InterruptTrackingParser.interrupted.set(false);

    TextExtractor stage2 = (TextExtractor) factory.get("TextExtractorTest/notimeout.conf");

    Document doc2 = Document.create("doc2");
    doc2.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage2.processDocument(doc2);

    assertFalse("Parser should not have been interrupted", InterruptTrackingParser.interrupted.get());
    // Document should have text after processing without interruption.
    assertEquals("Document should have text.", "Hi There!\n", doc2.getString("text"));
  }
}