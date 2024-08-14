package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ChunkingTest {

  private StageFactory factory = StageFactory.of(Chunking.class);

  /*
  Cases to test:
  - default
    - sentence splitter
    - paragraph splitter
    - regex splitter
  - edge cases
    - sentence too long
    - paragraph too long
    - regex too long
  - overlap
   */


  @Test
  public void testSentenceChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/config.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a sentence. This is the second sentence. This is the third sentence.");

    stage.processDocument(doc);

    System.out.println(doc);
  }

  @Test
  public void testLongSentenceChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/config.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a sentence so long that it passes the token limit.");

    stage.processDocument(doc);

    System.out.println(doc);
  }


  @Test
  public void testParagraphChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/paragraphChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the first paragraph of the document.\n\n This is the second paragraph of the document");

    stage.processDocument(doc);

    System.out.println(doc);
  }



  @Test
  public void testLongParagraphChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/paragraphChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a very long first paragraph in document. This second sentence tries to exceed the token limit.\n\n This is the second paragraph of the document");

    stage.processDocument(doc);

    System.out.println(doc);
  }


  @Test
  public void testLongParagraphChunking2() throws StageException {
  }


}
