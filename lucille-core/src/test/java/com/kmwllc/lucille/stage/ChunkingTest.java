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
    Stage stage = factory.get("ChunkingTest/sentenceChunk.conf");
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
  public void testSentenceChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/sentenceChunk.conf");
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
  public void testSentenceChunkingWithLimit() throws StageException {
    Stage stage = factory.get("ChunkingTest/sentenceChunkWithLimit.conf");
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
  public void testMerge3Overlap1() throws StageException {
    // chunk size 3, overlap 1
    Stage stage = factory.get("ChunkingTest/testMerge3Overlap1.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    Document child5 = childrenDocs.get(4);
    Document child6 = childrenDocs.get(5);

    assertEquals("The sun is bright. The sky is clear. Birds are singing.", child1.getString("chunk"));
    assertEquals("Birds are singing. A dog barks. Children play.", child2.getString("chunk"));
    assertEquals("Children play. Trees sway. Leaves rustle.", child3.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool.", child4.getString("chunk"));
    assertEquals("The breeze is cool. Flowers bloom. A cat sleeps.", child5.getString("chunk"));
    assertEquals("A cat sleeps. The clock ticks.", child6.getString("chunk"));
  }


  @Test
  public void testMerge3Overlap2() throws StageException {
    // chunk size 3, overlap 1
    Stage stage = factory.get("ChunkingTest/testMerge3Overlap2.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    Document child5 = childrenDocs.get(4);
    Document child6 = childrenDocs.get(5);
    Document child7 = childrenDocs.get(6);
    Document child8 = childrenDocs.get(7);
    Document child9 = childrenDocs.get(8);
    Document child10 = childrenDocs.get(9);

    assertEquals("The sun is bright. The sky is clear. Birds are singing.", child1.getString("chunk"));
    assertEquals("The sky is clear. Birds are singing. A dog barks.", child2.getString("chunk"));
    assertEquals("Birds are singing. A dog barks. Children play.", child3.getString("chunk"));
    assertEquals("A dog barks. Children play. Trees sway.", child4.getString("chunk"));
    assertEquals("Children play. Trees sway. Leaves rustle.", child5.getString("chunk"));
    assertEquals("Trees sway. Leaves rustle. A car drives by.", child6.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool.", child7.getString("chunk"));
    assertEquals("A car drives by. The breeze is cool. Flowers bloom.", child8.getString("chunk"));
    assertEquals("The breeze is cool. Flowers bloom. A cat sleeps.", child9.getString("chunk"));
    assertEquals("Flowers bloom. A cat sleeps. The clock ticks.", child10.getString("chunk"));
  }


  @Test
  public void testMerge4Overlap2() throws StageException {
    // chunk size 4, overlap 2
    Stage stage = factory.get("ChunkingTest/testMerge4Overlap2.conf");
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
    Document child6 = childrenDocs.get(5);

    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks.", child1.getString("chunk"));
    assertEquals("Birds are singing. A dog barks. Children play. Trees sway.", child2.getString("chunk"));
    assertEquals("Children play. Trees sway. Leaves rustle. A car drives by.", child3.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool. Flowers bloom.", child4.getString("chunk"));
    assertEquals("The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.", child5.getString("chunk"));
    assertEquals("A cat sleeps. The clock ticks. The phone rings.", child6.getString("chunk"));
  }


  @Test
  public void testMerge4NoOverlap() throws StageException {
    // chunk size 4, overlap 3
    Stage stage = factory.get("ChunkingTest/testMerge4NoOverlap.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks. A phone rings.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);
    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks.", child1.getString("chunk"));
    assertEquals("Children play. Trees sway. Leaves rustle. A car drives by.", child2.getString("chunk"));
    assertEquals("The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.", child3.getString("chunk"));
    assertEquals("A phone rings.", child4.getString("chunk"));
  }


  @Test
  public void testMerge4Overlap1() throws StageException {
    // chunk size 4, overlap 1
    Stage stage = factory.get("ChunkingTest/testMerge4Overlap1.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks.", child1.getString("chunk"));
    assertEquals("A dog barks. Children play. Trees sway. Leaves rustle.", child2.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool. Flowers bloom.", child3.getString("chunk"));
    assertEquals("Flowers bloom. A cat sleeps. The clock ticks.", child4.getString("chunk"));
  }

  @Test
  public void testMerge5Overlap2() throws StageException {
    // chunk size 5, overlap 2
    Stage stage = factory.get("ChunkingTest/testMerge5Overlap2.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play.", child1.getString("chunk"));
    assertEquals("A dog barks. Children play. Trees sway. Leaves rustle. A car drives by.", child2.getString("chunk"));
    assertEquals("Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps.", child3.getString("chunk"));
    assertEquals("Flowers bloom. A cat sleeps. The clock ticks.", child4.getString("chunk"));
  }

  @Test
  public void testMerge5Overlap1() throws StageException {
    // chunk size 5, overlap 1
    Stage stage = factory.get("ChunkingTest/testMerge5Overlap1.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);

    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play.", child1.getString("chunk"));
    assertEquals("Children play. Trees sway. Leaves rustle. A car drives by. The breeze is cool.", child2.getString("chunk"));
    assertEquals("The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks.", child3.getString("chunk"));
  }

  @Test
  public void testMerge5NoOverlap() throws StageException {
    // chunk size 4, overlap 3
    Stage stage = factory.get("ChunkingTest/testMerge5NoOverlap.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play. Trees sway."
        + " Leaves rustle. A car drives by. The breeze is cool. Flowers bloom. A cat sleeps. The clock ticks. A phone rings. The clock ticks.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    assertEquals("The sun is bright. The sky is clear. Birds are singing. A dog barks. Children play.", child1.getString("chunk"));
    assertEquals("Trees sway. Leaves rustle. A car drives by. The breeze is cool. Flowers bloom.", child2.getString("chunk"));
    assertEquals("A cat sleeps. The clock ticks. A phone rings. The clock ticks.", child3.getString("chunk"));
  }

  @Test
  public void testOverlapInvalid() throws StageException {
    // new chunk size 4 for old 3 chunks
    Stage stage = factory.get("ChunkingTest/testMerge4NoOverlap.conf");
    stage.start();

    Document doc = Document.create("id");
    String sampleText = "The sun is bright. The sky is clear. Birds are singing.";

    doc.setField("text", sampleText);

    stage.processDocument(doc);
    List<Document> childrenDocs = doc.getChildren();

    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);

    assertEquals("The sun is bright.", child1.getString("chunk"));
    assertEquals("The sky is clear.", child2.getString("chunk"));
    assertEquals("Birds are singing.", child3.getString("chunk"));
  }

  @Test
  public void testParagraphChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/paragraphChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the first\nparagraph of the document.\n\n This is the second paragraph of the document."
        + "\n \n This is the third paragraph of the document. \n \n This is the fourth paragraph of the document.");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("This is the first paragraph of the document.", child1.getString("chunk"));
    assertEquals("This is the second paragraph of the document.", child2.getString("chunk"));
    assertEquals("This is the third paragraph of the document.", child3.getString("chunk"));
    assertEquals("This is the fourth paragraph of the document.", child4.getString("chunk"));
  }



  @Test
  public void testParagraphChunkingWithLimits() throws StageException {
    Stage stage = factory.get("ChunkingTest/paragraphChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is a very long first paragraph in document. This second sentence exceeds the character limit.\n\n This is the second paragraph of the document");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);

    assertEquals("This is a very long first paragraph in document. This second", child1.getString("chunk"));
    assertEquals("This is the second paragraph of the document", child2.getString("chunk"));
  }

  @Test
  public void testFixedChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/fixedChunk.conf");
    stage.start();

    Document doc = Document.create("id");
    doc.setField("text", "This is the\nfirst paragraph.\n\nThis is the second paragraph");

    stage.processDocument(doc);

    List<Document> childrenDocs = doc.getChildren();
    Document child1 = childrenDocs.get(0);
    Document child2 = childrenDocs.get(1);
    Document child3 = childrenDocs.get(2);
    Document child4 = childrenDocs.get(3);

    assertEquals("This is the fir", child1.getString("chunk"));
    assertEquals("st paragraph.  ", child2.getString("chunk"));
    assertEquals("This is the sec", child3.getString("chunk"));
    assertEquals("ond paragraph", child4.getString("chunk"));
    assertEquals(15, child1.getString("chunk").length());
    assertEquals(15, child2.getString("chunk").length());
    assertEquals(15, child3.getString("chunk").length());
    // remainder of the content is lesser than the character limit
    assertEquals(13, child4.getString("chunk").length());
  }



  @Test
  public void testRegexChunking() throws StageException {
    Stage stage = factory.get("ChunkingTest/regexChunk.conf");
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


}
