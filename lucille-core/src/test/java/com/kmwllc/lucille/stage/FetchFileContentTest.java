package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
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
}
