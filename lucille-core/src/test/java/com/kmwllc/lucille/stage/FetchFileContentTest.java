package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.Test;

public class FetchFileContentTest {

  private StageFactory factory = StageFactory.of(FetchFileContent.class);

  Path helloPath = Paths.get("src/test/resources/FetchFileContentTest/hello.txt");
  Path goodbyePath = Paths.get("src/test/resources/FetchFileContentTest/goodbye.txt");

  byte[] helloContents = Files.readAllBytes(helloPath);
  byte[] goodbyeContents = Files.readAllBytes(goodbyePath);

  public FetchFileContentTest() throws IOException { }

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
}
