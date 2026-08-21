package com.kmwllc.lucille.tika.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.atLeastOnce;
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
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AbstractParser;
import org.apache.tika.parser.DefaultParser;
import org.apache.tika.parser.ParseContext;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

      // verify the fetched inputStream is closed (no leak). The stream is both wrapped in a TikaInputStream and
      // declared directly in the try-with-resources for exception safety, so close() may be invoked more than once;
      // close() is idempotent, so we only assert it happens at least once.
      verify(inputStream, atLeastOnce()).close();
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

  // when fieldNamesField is set, all of the metadata fields should be put into that field
  @Test
  public void testFieldNamesField() throws StageException {
    Stage stage = factory.get("TextExtractorTest/fieldnames.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    List<String> fieldNames = doc.getStringList("tika_property_names");
    assertTrue(fieldNames.contains("tika_content_type"));
    assertTrue(fieldNames.contains("tika_content_encoding"));
    assertTrue(fieldNames.contains("tika_x_tika_parsed_by"));

    // make sure every field appears exactly once and that we got everything
    Set<String> metadataFieldsOnDoc = doc.getFieldNames().stream()
        .filter(name -> name.startsWith("tika_"))
        .filter(name -> !name.equals("tika_property_names"))
        .collect(Collectors.toSet());
    assertEquals(metadataFieldsOnDoc, Set.copyOf(fieldNames));
    assertEquals(metadataFieldsOnDoc.size(), fieldNames.size());
  }

  // test that fieldNamesField respects whitelist / blacklist
  @Test
  public void testFieldNamesFieldRespectsWhitelist() throws StageException {
    Stage stage = factory.get("TextExtractorTest/fieldnames-whitelist.conf");
    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    List<String> fieldNames = doc.getStringList("tika_property_names");
    assertEquals(List.of("tika_content_type"), fieldNames);
  }

  // when metadataFields is set, the referenced document fields' values should be copied into the Metadata
  // handed to Tika (used, for example, to supply content-type detection hints such as resourceName / Content-Type).
  @Test
  public void testMetadataFieldsWiredIn() throws StageException {
    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/metadatafields.conf");

    Document doc = Document.create("doc1");
    doc.setField("filename", "myfile.csv");
    doc.setField("content_type", "text/plain");

    Metadata metadata = stage.createMetadataInput(doc);

    // the map values ("filename", "content_type") are document field NAMES; the document's VALUES are wired in
    // under the configured Tika keys.
    assertEquals("myfile.csv", metadata.get("resourceName"));
    assertEquals("text/plain", metadata.get("Content-Type"));
  }

  // multi-valued document fields should contribute all of their values to the input Metadata
  @Test
  public void testMetadataFieldsMultiValued() throws StageException {
    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/metadatafields.conf");

    Document doc = Document.create("doc1");
    doc.setField("filename", "first.csv");
    doc.addToField("filename", "second.csv");

    Metadata metadata = stage.createMetadataInput(doc);

    assertArrayEquals(new String[]{"first.csv", "second.csv"}, metadata.getValues("resourceName"));
  }

  // document fields referenced by metadataFields but absent from the document should be skipped, not error
  @Test
  public void testMetadataFieldsMissingDocField() throws StageException {
    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/metadatafields.conf");

    Document doc = Document.create("doc1");
    doc.setField("filename", "myfile.csv");
    // note: no "content_type" field on the document

    Metadata metadata = stage.createMetadataInput(doc);

    assertEquals("myfile.csv", metadata.get("resourceName"));
    assertNull(metadata.get("Content-Type"));
  }

  // when metadataFields is not configured, the input Metadata should be empty
  @Test
  public void testMetadataFieldsNotConfigured() throws StageException {
    TextExtractor stage = (TextExtractor) factory.get("TextExtractorTest/filepath.conf");

    Metadata metadata = stage.createMetadataInput(Document.create("doc1"));

    assertEquals(0, metadata.names().length);
  }

  // end-to-end: the wired-in resourceName hint reaches the parser. Tika does not overwrite resourceName, so the value
  // we injected round-trips into the extracted metadata, proving the input Metadata built from metadataFields was
  // actually handed to the parser (this is the hook Tika uses for filename-based content-type detection).
  @Test
  public void testMetadataFieldsHintReachesParser() throws StageException, IOException {
    Stage stage = factory.get("TextExtractorTest/metadatafields.conf");

    Document doc = Document.create("doc1");
    byte[] fileContent = Files.readAllBytes(new File("src/test/resources/TextExtractorTest/tika.txt").toPath());
    doc.setField("byte_array", fileContent);
    doc.setField("filename", "myfile.csv");

    stage.processDocument(doc);

    // the hint round-tripped through Tika's metadata, confirming it was passed to the parser
    assertEquals("myfile.csv", doc.getString("tika_resourcename"));
  }

  // processDocument should wrap the file-path content in a TikaInputStream before handing it to the parser, so
  // container-aware detectors can spool and inspect the content (a plain InputStream limits detection to Mime Magic).
  @Test
  public void testFilePathWrappedInTikaInputStream() throws Exception {
    TextExtractor stage = spy((TextExtractor) factory.get("TextExtractorTest/filepath.conf"));
    stage.start();

    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
    verify(stage).parseInputStream(any(Metadata.class), eq(doc), streamCaptor.capture());
    assertTrue("stream handed to the parser should be a TikaInputStream, was: " + streamCaptor.getValue().getClass(),
        streamCaptor.getValue() instanceof TikaInputStream);

    stage.stop();
  }

  // same as above, for the byteArray path
  @Test
  public void testByteArrayWrappedInTikaInputStream() throws Exception {
    TextExtractor stage = spy((TextExtractor) factory.get("TextExtractorTest/bytearray.conf"));
    stage.start();

    Document doc = Document.create("doc1");
    doc.setField("byte_array", Files.readAllBytes(new File("src/test/resources/TextExtractorTest/tika.txt").toPath()));

    stage.processDocument(doc);

    ArgumentCaptor<InputStream> streamCaptor = ArgumentCaptor.forClass(InputStream.class);
    verify(stage).parseInputStream(any(Metadata.class), eq(doc), streamCaptor.capture());
    assertTrue("stream handed to the parser should be a TikaInputStream, was: " + streamCaptor.getValue().getClass(),
        streamCaptor.getValue() instanceof TikaInputStream);

    stage.stop();
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
        // Block for a long time (10s) to reduce risk of a racy test.
        // We don't want some race condition where this parse is able to complete causing the test to fail.
        // Interrupt should cut the sleep short - we do not wait for this entire time.
        Thread.sleep(10_000);
        super.parse(stream, handler, metadata, context);
      } catch (InterruptedException e) {
        interrupted.set(true);
      }
    }
  }

  @Test
  public void testTimeout() throws Exception {
    InterruptTrackingParser.interrupted.set(false);

    Stage stage = factory.get("TextExtractorTest/timeout.conf");

    Document doc = Document.create("doc1");
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);

    // poll for the parser to be interrupted. give it max ~5 sec. to do so
    Instant deadline = Instant.now().plus(5, ChronoUnit.SECONDS);
    while (!InterruptTrackingParser.interrupted.get() && Instant.now().isBefore(deadline)) {
      Thread.sleep(50);
    }

    // If timeout works, it should have returned within much less than our 5 sec. max
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

  @Test
  public void testForkingParser() throws Exception {
    Stage stage = factory.get("TextExtractorTest/forking.conf");
    Document doc = Document.create("doc1");

    // set path as absolute Path
    doc.setField("path", Paths.get("src/test/resources/TextExtractorTest/tika.txt").toAbsolutePath().toString());

    stage.processDocument(doc);
    assertEquals("Hi There!\n", doc.getString("text"));
  }

  @Test
  public void testForkingParserWithMemoryHogParser() throws Exception {
    Stage stage = factory.get("TextExtractorTest/forking-oom.conf");
    byte[] tikaTxtBytes = Files.readAllBytes(Paths.get("src/test/resources/TextExtractorTest/tika.txt"));
    // magic bytes for a zip local file header, routed to MemoryHogParser by tika-config-oom.xml
    byte[] zipMagicBytes = new byte[]{0x50, 0x4B, 0x03, 0x04};

    Document doc1 = Document.create("doc1");
    doc1.setField("content", tikaTxtBytes);
    stage.processDocument(doc1);
    assertEquals("Hi There!\n", doc1.getString("text"));

    Document doc2 = Document.create("doc2");
    doc2.setField("content", zipMagicBytes);
    // OOM exception causes a specific doc failure.
    assertThrows(StageException.class, () -> stage.processDocument(doc2));

    // note that our JVM is still fully operational!
    Document doc3 = Document.create("doc3");
    doc3.setField("content", tikaTxtBytes);
    stage.processDocument(doc3);
    assertEquals("Hi There!\n", doc3.getString("text"));
  }

  /**
   * Sanity check that a TikaConfig is built once per config path and reused across stage instances,
   * rather than rebuilt for every stage (as would happen with each worker thread in a real run).
   */
  @Test
  public void testTikaConfigCaching() throws Exception {
    // Clear the JVM-wide cache so this test doesn't depend on ordering with other tests, and so we
    // don't leave a mock config behind for them afterward.
    Field cacheField = TextExtractor.class.getDeclaredField("TIKA_CONFIG_CACHE");
    cacheField.setAccessible(true);
    Map<?, ?> cache = (Map<?, ?>) cacheField.get(null);
    cache.clear();

    // AutoDetectParser(TikaConfig) reads these four getters off the config, so delegate them to a
    // real default config to let the mock stand in without changing what the stage builds.
    TikaConfig realConfig = TikaConfig.getDefaultConfig();
    try (MockedConstruction<TikaConfig> mockedConstruction = mockConstruction(TikaConfig.class,
        (mock, context) -> {
          when(mock.getMediaTypeRegistry()).thenReturn(realConfig.getMediaTypeRegistry());
          when(mock.getParser()).thenReturn(realConfig.getParser());
          when(mock.getDetector()).thenReturn(realConfig.getDetector());
          when(mock.getAutoDetectParserConfig()).thenReturn(realConfig.getAutoDetectParserConfig());
        })) {
      // Two stages pointing at the same tikaConfigPath, as two worker threads would each be. Each
      // factory.get(...) calls start(), which is what resolves the config.
      factory.get("TextExtractorTest/tika-config.conf");
      factory.get("TextExtractorTest/tika-config.conf");

      // The config is built for the first stage and served from the cache for the second.
      assertEquals(1, mockedConstruction.constructed().size());
    } finally {
      cache.clear();
    }
  }

  /**
   * Registered (see tika-config-oom.xml) as the parser for application/zip only, so it doesn't
   * interfere with normal text/plain parsing. Deliberately allocates memory until it dies, so
   * the resulting OutOfMemoryError doesn't depend on tuning a heap size against any particular
   * file's actual parse-time memory footprint.
   */
  public static class MemoryHogParser extends AbstractParser {

    @Override
    public Set<MediaType> getSupportedTypes(ParseContext context) {
      return Collections.singleton(MediaType.application("zip"));
    }

    @Override
    public void parse(InputStream stream, ContentHandler handler, Metadata metadata, ParseContext context)
        throws TikaException, IOException, SAXException {
      List<byte[]> chunks = new ArrayList<>();
      while (true) {
        chunks.add(new byte[10_000_000]);
      }
    }
  }
}