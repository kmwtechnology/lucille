package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.util.List;
import java.util.Optional;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ChunkingTest {

  private StageFactory factory = StageFactory.of(Chunking.class);

  @Test
  public void childrenInformation() throws StageException {
    Stage stage = factory.get("ChunkingTest/testSentenceChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a sentence. This is the second sentence. This is the third sentence.");

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);

    assertEquals("id-1", child1.getString("id"));
    assertEquals("id-2", child2.getString("id"));
    assertEquals("id-3", child3.getString("id"));

    assertEquals(Integer.valueOf(1), child1.getInt("chunk_number"));
    assertEquals(Integer.valueOf(2), child2.getInt("chunk_number"));
    assertEquals(Integer.valueOf(3), child3.getInt("chunk_number"));

    assertEquals(Integer.valueOf(3), child1.getInt("total_chunks"));
    assertEquals(Integer.valueOf(3), child2.getInt("total_chunks"));
    assertEquals(Integer.valueOf(3), child3.getInt("total_chunks"));

    assertEquals(Integer.valueOf(19), child1.getInt("length"));
    assertEquals(Integer.valueOf(28), child2.getInt("length"));
    assertEquals(Integer.valueOf(27), child3.getInt("length"));

    assertEquals(Integer.valueOf(0), child1.getInt("offset"));
    assertEquals(Integer.valueOf(19), child2.getInt("offset"));
    assertEquals(Integer.valueOf(47), child3.getInt("offset"));
  }

  @Test
  public void testCleanChunks() throws StageException {
    Stage stage = factory.get("ChunkingTest/testCleanChunk.conf");
    stage.start();
    Document doc = Document.create("id");
    doc.setField("text", "This is\na sentence.   \n\n This is the second sentence.       ");
    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    assertEquals("This is a sentence.", child1.getString("chunk"));
    assertEquals("This is the second sentence.", child2.getString("chunk"));
  }

  @Test
  public void testSentenceChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/testSentenceChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a sentence. This is the second sentence. This is the third sentence.");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);

    assertEquals("This is a sentence.", child1.getString("chunk"));
    assertEquals("This is the second sentence.", child2.getString("chunk"));
    assertEquals("This is the third sentence.", child3.getString("chunk"));
  }

  @Test
  public void testFixedChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/testFixedChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the\nfirst paragraph.\n\nThis is the second paragraph");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("This is the\nfir", child1.getString("chunk"));
    assertEquals("st paragraph.\n\n", child2.getString("chunk"));
    assertEquals("This is the sec", child3.getString("chunk"));
    assertEquals("ond paragraph", child4.getString("chunk"));
    assertEquals(15, child1.getString("chunk").length());
    assertEquals(15, child2.getString("chunk").length());
    assertEquals(15, child3.getString("chunk").length());
    // remainder of the content is lesser than the character limit
    assertEquals(13, child4.getString("chunk").length());
  }

  @Test
  public void testParagraphChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/testParagraphChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the first\nparagraph of the document.\n\n This is the second paragraph of the document."
        + "\n \nThis is the third paragraph of the document. \n \n This is the fourth paragraph of the document.");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("This is the first\nparagraph of the document.", child1.getString("chunk"));
    assertEquals("This is the second paragraph of the document.", child2.getString("chunk"));
    assertEquals("This is the third paragraph of the document.", child3.getString("chunk"));
    assertEquals("This is the fourth paragraph of the document.", child4.getString("chunk"));
  }

  @Test
  public void testRegexChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/testRegexChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the first sentence, with a comma. This is the second sentence, with another comma");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);

    assertEquals("This is the first sentence", child1.getString("chunk"));
    assertEquals(" with a comma. This is the second sentence", child2.getString("chunk"));
    assertEquals(" with another comma", child3.getString("chunk"));
  }

  @Test
  public void testCharacterLimit() throws StageException {
    Stage stage = factory.get("ChunkingTest/testCharacterLimit.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a sentence. This is a sentence so long that it passes the character limit.");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);

    assertEquals("This is a sentence.", child1.getString("chunk"));
    assertEquals("This is a sentence so long that it passe", child2.getString("chunk"));
  }

  @Test
  public void testMerging() throws StageException {
    Stage stage = factory.get("ChunkingTest/testMerging.conf");
    stage.start();
    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks. The phone rings.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    Document child5 = childrenDocs.get(4);

    assertEquals("The sun is bright. The sky is clear. Birds are singing.", child1.getString("chunk"));
    assertEquals("A dog barks. Children play. Trees sway.", child2.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool.", child3.getString("chunk"));
    assertEquals("Flowers bloom. A cat sleeps. The clock ticks.", child4.getString("chunk"));
    assertEquals("The phone rings.", child5.getString("chunk"));
  }

  @Test
  public void testMerging2() throws StageException {
    Stage stage = factory.get("ChunkingTest/testMerging.conf");
    stage.start();
    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    // merging is set to 3, but only expected to merge the only 2 sentences into 1
    assertEquals("The sun is bright. The sky is clear.", child1.getString("chunk"));
  }

  @Test
  public void testFiltering() throws StageException {
    Stage stage = factory.get("ChunkingTest/testFiltering.conf");
    stage.start();
    Document doc = Document.create("id");

    String sampleText = "The sun is bright. 3.\n The phone rings.";
    doc.setField("text", sampleText);
    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    assertEquals("The sun is bright.", child1.getString("chunk"));
    assertEquals("The phone rings.", child2.getString("chunk"));
  }

  @Test
  public void testPreMergeTruncate() throws StageException {
    Stage stage = factory.get("ChunkingTest/testPreMergeTruncate.conf");
    stage.start();


    Document doc = Document.create("id");

    String sampleText = "This is the first sentence. This sentence is too long and will be truncated before merging. "
        + "This is the other sentence.";

    doc.setField("text", sampleText);
    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    assertEquals("This is the first sentence. This sentence is too long and This is the other sentence.", child1.getString("chunk"));
  }

  @Test
  public void testOverlap() throws StageException {
    Stage stage = factory.get("ChunkingTest/testOverlap.conf");
    stage.start();
    Document doc = Document.create("id");
    String sampleText = "The sun is bright, The sky is clear. Birds are singing, a dog barks, children play. Trees sway,"
        + " leaves rustle, a car drives by. The breeze is cool, flowers bloom, a cat sleeps. The clock ticks, the phone rings.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    for (Document child : childrenDocs) {
      System.out.println(child.getString("chunk"));
    }
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    Document child5 = childrenDocs.get(4);

    assertEquals("The sun is bright, The sky is clear. Birds a", child1.getString("chunk"));
    assertEquals("is clear. Birds are singing, a dog barks, children play. Trees swa", child2.getString("chunk"));
    assertEquals("en play. Trees sway, leaves rustle, a car drives by. The bree", child3.getString("chunk"));
    assertEquals("rives by. The breeze is cool, flowers bloom, a cat sleeps. The clock", child4.getString("chunk"));
    assertEquals("leeps. The clock ticks, the phone rings.", child5.getString("chunk"));
  }


  @Test
  public void testMerge3Overlap20() throws StageException {
    // chunk size 3, overlap 20 percent
    Stage stage = factory.get("ChunkingTest/testMerge3Overlap20.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks. The phone rings.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    Document child5 = childrenDocs.get(4);

    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks", child1.getString("chunk"));
    assertEquals("inging. A dog barks. Children play. Trees sway. Leaves", child2.getString("chunk"));
    assertEquals("rees sway. Leaves rustle. A car drives by. The breeze is cool. Flowers bl", child3.getString("chunk"));
    assertEquals("is cool. Flowers bloom. A cat sleeps. The clock ticks. The phone", child4.getString("chunk"));
    assertEquals("ks. The phone rings.", child5.getString("chunk"));
  }
}