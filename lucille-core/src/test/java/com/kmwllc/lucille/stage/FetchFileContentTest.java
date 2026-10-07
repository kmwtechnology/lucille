package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import org.mockito.Mockito;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import org.junit.Test;

public class FetchFileContentTest {

  private final StageFactory factory = StageFactory.of(FetchFileContent.class);

  private final Path helloPath = Paths.get("src/test/resources/FetchFileContentTest/hello.txt");
  private final Path goodbyePath = Paths.get("src/test/resources/FetchFileContentTest/goodbye.txt");

  private final byte[] helloContents;
  private final byte[] goodbyeContents;

  public FetchFileContentTest() throws IOException {
    helloContents = Files.readAllBytes(helloPath);
    goodbyeContents = Files.readAllBytes(goodbyePath);
  }

  @Test
  public void testFetchFileContent() throws StageException {
    Stage stage = factory.get(ConfigFactory.empty());

    Document doc1 = Document.create("test_hello");
    doc1.setField("file_path", helloPath.toString());

    Document doc2 = Document.create("test_goodbye");
    doc2.setField("file_path", goodbyePath.toString());

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertTrue(doc1.has("file_content"));
    assertArrayEquals(helloContents, doc1.getBytes("file_content"));

    assertTrue(doc2.has("file_content"));
    assertArrayEquals(goodbyeContents, doc2.getBytes("file_content"));
  }

  @Test
  public void testAlternateFilePath() throws StageException {
    Stage stage = factory.get(ConfigFactory.parseMap(Map.of("filePathField", "source")));

    Document doc1 = Document.create("good_hello");
    doc1.setField("source", helloPath.toString());

    Document doc2 = Document.create("bad_goodbye");
    doc2.setField("file_path", goodbyePath.toString());

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertTrue(doc1.has("file_content"));
    assertArrayEquals(helloContents, doc1.getBytes("file_content"));

    assertFalse(doc2.has("file_content"));
  }

  @Test
  public void testAlternateContentField() throws StageException {
    Stage stage = factory.get(ConfigFactory.parseMap(Map.of("fileContentField", "contents")));

    Document doc1 = Document.create("test_hello");
    doc1.setField("file_path", helloPath.toString());

    Document doc2 = Document.create("test_goodbye");
    doc2.setField("file_path", goodbyePath.toString());

    stage.processDocument(doc1);
    stage.processDocument(doc2);

    assertTrue(doc1.has("contents"));
    assertFalse(doc1.has("file_content"));
    assertArrayEquals(helloContents, doc1.getBytes("contents"));

    assertTrue(doc2.has("contents"));
    assertFalse(doc2.has("file_content"));
    assertArrayEquals(goodbyeContents, doc2.getBytes("contents"));
  }

  @Test
  public void testOverwriteContents() throws StageException {
    Stage stage = factory.get(ConfigFactory.empty());

    Document doc = Document.create("test_hello");
    doc.setField("file_path", helloPath.toString());
    doc.setField("file_content", new byte[] {1, 2, 3});

    stage.processDocument(doc);

    assertArrayEquals(helloContents, doc.getBytes("file_content"));
  }

  @Test
  public void testMissingProvider() throws StageException {
    Stage stage = factory.get(ConfigFactory.empty());

    Document doc = Document.create("test_hello");
    doc.setField("file_path", "s3://bucket/hello.txt");

    assertThrows(StageException.class, () -> stage.processDocument(doc));
  }

  private FetchFileContent stageWithStream(Config config, InputStream in) throws IOException {
    FileContentFetcher fetcher = Mockito.mock(FileContentFetcher.class);
    Mockito.when(fetcher.getInputStream(Mockito.anyString())).thenReturn(in);
    return new FetchFileContent(config, fetcher);
  }

  private static class TrackingStream extends ByteArrayInputStream {
    final AtomicBoolean closed = new AtomicBoolean(false);
    final boolean failOnRead;

    TrackingStream(byte[] b, boolean failOnRead) {
      super(b);
      this.failOnRead = failOnRead;
    }

    @Override
    public synchronized byte[] readAllBytes() {
      if (failOnRead) {
        throw new RuntimeException("read failed");
      }
      return super.readAllBytes();
    }

    @Override
    public void close() throws IOException {
      closed.set(true);
      super.close();
    }
  }

  private Document docWithPath() {
    Document doc = Document.create("d");
    doc.setField("file_path", "ignored");
    return doc;
  }

  @Test
  public void testStreamClosedOnSuccess() throws Exception {
    TrackingStream in = new TrackingStream(new byte[] {1, 2, 3}, false);
    Document doc = docWithPath();
    stageWithStream(ConfigFactory.empty(), in).processDocument(doc);
    assertTrue(in.closed.get());
    assertArrayEquals(new byte[] {1, 2, 3}, doc.getBytes("file_content"));
  }

  @Test
  public void testStreamClosedOnException() throws Exception {
    TrackingStream in = new TrackingStream(new byte[] {1, 2, 3}, true);
    Stage stage = stageWithStream(ConfigFactory.empty(), in);
    assertThrows(RuntimeException.class, () -> stage.processDocument(docWithPath()));
    assertTrue(in.closed.get());
  }

  @Test
  public void testMaxSizeUnderAtAndOver() throws Exception {
    byte[] data = new byte[10];
    Config config = ConfigFactory.parseMap(Map.of("maxSizeBytes", 10));

    Document underLimitDoc = docWithPath();
    stageWithStream(ConfigFactory.parseMap(Map.of("maxSizeBytes", 11)), new ByteArrayInputStream(data))
        .processDocument(underLimitDoc);
    assertEquals(10, underLimitDoc.getBytes("file_content").length);

    Document atLimitDoc = docWithPath();
    stageWithStream(config, new ByteArrayInputStream(data)).processDocument(atLimitDoc);
    assertEquals(10, atLimitDoc.getBytes("file_content").length);

    TrackingStream in = new TrackingStream(new byte[11], false);
    Document overLimitDoc = docWithPath();
    Stage stage = stageWithStream(config, in);
    assertThrows(StageException.class, () -> stage.processDocument(overLimitDoc));
    assertFalse(overLimitDoc.has("file_content"));
    assertTrue(in.closed.get());
  }

  @Test
  public void testLimitDoesNotRelyOnSizeLookup() throws Exception {
    // a stream much larger than the limit is only read up to limit + 1 bytes
    TrackingStream in = new TrackingStream(new byte[100_000], false);
    Stage stage = stageWithStream(ConfigFactory.parseMap(Map.of("maxSizeBytes", 5)), in);
    assertThrows(StageException.class, () -> stage.processDocument(docWithPath()));
    assertEquals(100_000 - 6, in.available());
  }

  @Test
  public void testInvalidMaxSize() {
    assertThrows(StageException.class, () -> factory.get(ConfigFactory.parseMap(Map.of("maxSizeBytes", 0))));
    assertThrows(StageException.class, () -> factory.get(ConfigFactory.parseMap(Map.of("maxSizeBytes", -5))));
    assertThrows(StageException.class, () -> factory.get(ConfigFactory.parseMap(Map.of("maxSizeBytes", 5000000000L))));
  }
}
