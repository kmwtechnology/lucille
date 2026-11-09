package com.kmwllc.lucille.stage;

import static java.util.Arrays.stream;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.output.Response;
import java.util.List;
import org.junit.Test;
import org.mockito.Mockito;

public class OpenAIEmbedTest {

  private StageFactory factory = StageFactory.of(OpenAIEmbed.class);

  private static final float DELTA = 1e-6f; // Tolerance level for floating point comparison

  @Test
  public void testOpenAIEmptyAPIKey() throws Exception {
    assertThrows(StageException.class, () -> {
      factory.get("OpenAIEmbedTest/APIKeyEmpty.conf");
    });
  }

  @Test
  public void testOpenAIBothEmbedFalse() throws Exception {
    assertThrows(StageException.class, () -> {
      factory.get("OpenAIEmbedTest/bothEmbedFalse.conf");
    });
  }

  @Test
  public void testOpenAIModelNameMisspelledRetrieval() throws Exception {
    // a misspelled or unsupported model name is a configuration error; it used to fall back silently to
    // text-embedding-3-small
    assertThrows(StageException.class, () -> factory.get("OpenAIEmbedTest/wrongModelName.conf"));
  }

  @Test
  public void testSkipDocumentsWithoutTextField() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class).get("OpenAIEmbedTest/skipDocumentsWithoutTextField.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);
    stage.setModel(model);

    Document parent = Document.create("parent");
    Document child = Document.create("child");
    parent.addChild(child);

    stage.processDocument(parent);

    assertFalse(parent.has("embeddings"));
    assertFalse(child.has("embeddings"));
  }


  @Test
  public void testEmbedChildOnly() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class).get("OpenAIEmbedTest/embedChildDoc.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);
    Embedding embedding = mock(Embedding.class);
    float[] vectors = new float[] {0.1F, 0.2F, 0.3F, 0.4F, 0.5f};
    when(embedding.vector()).thenReturn(vectors);

    when(model.embedAll(Mockito.any(List.class))).thenReturn(Response.from(List.of(embedding)));
    stage.setModel(model);

    Document parent = Document.create("parent");
    Document child = Document.create("child");
    child.setField("text", "This should be embedded");

    parent.addChild(child);

    stage.processDocument(parent);

    assertFalse(parent.has("embeddings"));

    // Verify child documents are replaced with copies (current behavior)
    List<Document> processedChildren = parent.getChildren();
    assertEquals(1, processedChildren.size());

    Document processedChild = processedChildren.get(0);

    // Child should have embeddings applied
    List<Float> actualEmbedding = processedChild.getFloatList("embeddings");
    assertEquals(vectors.length, actualEmbedding.size());
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(vectors[i], actualEmbedding.get(i), DELTA);
    }

    // Verify the child in the parent is a different object than the original
    // (since getChildren() returns copies and they get replaced)
    assertFalse("Child should be replaced with a copy", child == processedChild);
    assertEquals("Child ID should be preserved", child.getId(), processedChild.getId());
  }


  @Test
  public void testEmbedParentOnly() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class).get("OpenAIEmbedTest/embedParentDoc.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);
    Embedding embedding = mock(Embedding.class);
    float[] vectors = new float[] {0.1F, 0.2F, 0.3F, 0.4F, 0.5f};
    when(embedding.vector()).thenReturn(vectors);

    when(model.embedAll(Mockito.any(List.class))).thenReturn(Response.from(List.of(embedding)));
    stage.setModel(model);

    Document parent = Document.create("parent");
    parent.setField("text", "This should be embedded");
    Document child = Document.create("child");
    child.setField("text", "This should not be embedded");
    parent.addChild(child);

    stage.processDocument(parent);

    // test that parent has embeddings
    List<Float> actualEmbedding = parent.getFloatList("embeddings");
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(vectors[i], actualEmbedding.get(i), DELTA);
    }

    Document childDoc = parent.getChildren().get(0);
    assertFalse(childDoc.has("embeddings"));
  }


  @Test
  public void testEmbedParentAndChild() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class).get("OpenAIEmbedTest/embedChildAndParentDoc.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);
    Embedding embedding = mock(Embedding.class);
    float[] vectors = new float[] {0.1F, 0.2F, 0.3F, 0.4F, 0.5f};
    when(embedding.vector()).thenReturn(vectors);

    Embedding embedding2 = mock(Embedding.class);
    float[] vectors2 = new float[] {0.2F, 0.3F, 0.4F, 0.5F, 0.6f};
    when(embedding2.vector()).thenReturn(vectors2);

    when(model.embedAll(Mockito.any(List.class))).thenReturn(Response.from(List.of(embedding, embedding2)));
    stage.setModel(model);

    Document parent = Document.create("parent");
    parent.setField("text", "This should be embedded");
    Document child = Document.create("child");
    child.setField("text", "This should also be embedded");
    parent.addChild(child);

    stage.processDocument(parent);

    // Parent has embeddings (in-place modification)
    List<Float> parentEmbedding = parent.getFloatList("embeddings");
    assertEquals(vectors.length, parentEmbedding.size());
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(vectors[i], parentEmbedding.get(i), DELTA);
    }

    // Child is replaced with a copy and has embeddings applied
    List<Document> processedChildren = parent.getChildren();
    assertEquals(1, processedChildren.size());

    Document processedChild = processedChildren.get(0);

    // Child ID preserved
    assertEquals("child", processedChild.getId());

    // Child has correct embeddings (vectors2, not vectors)
    List<Float> childEmbedding = processedChild.getFloatList("embeddings");
    assertEquals(vectors2.length, childEmbedding.size());
    for (int i = 0; i < vectors2.length; i++) {
      assertEquals(vectors2[i], childEmbedding.get(i), DELTA);
    }

    // Verify child is replaced with a copy
    assertFalse("Child should be replaced with a copy", child == processedChild);
  }

  @Test
  public void testChildOrderPreservation() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class).get("OpenAIEmbedTest/embedChildDoc.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);

    // Create 3 different embeddings for 3 children
    Embedding embedding1 = mock(Embedding.class);
    when(embedding1.vector()).thenReturn(new float[] {0.1F, 0.2F});

    Embedding embedding2 = mock(Embedding.class);
    when(embedding2.vector()).thenReturn(new float[] {0.3F, 0.4F});

    Embedding embedding3 = mock(Embedding.class);
    when(embedding3.vector()).thenReturn(new float[] {0.5F, 0.6F});

    when(model.embedAll(Mockito.any(List.class)))
        .thenReturn(Response.from(List.of(embedding1, embedding2, embedding3)));
    stage.setModel(model);

    Document parent = Document.create("parent");

    // Add 3 children in a specific order
    Document child1 = Document.create("child1");
    child1.setField("text", "First child");
    parent.addChild(child1);

    Document child2 = Document.create("child2");
    child2.setField("text", "Second child");
    parent.addChild(child2);

    Document child3 = Document.create("child3");
    child3.setField("text", "Third child");
    parent.addChild(child3);

    stage.processDocument(parent);

    // Verify children are in the same order with correct embeddings
    List<Document> processedChildren = parent.getChildren();
    assertEquals(3, processedChildren.size());

    // First child should have embedding1
    assertEquals("child1", processedChildren.get(0).getId());
    assertEquals(0.1F, processedChildren.get(0).getFloatList("embeddings").get(0), DELTA);

    // Second child should have embedding2
    assertEquals("child2", processedChildren.get(1).getId());
    assertEquals(0.3F, processedChildren.get(1).getFloatList("embeddings").get(0), DELTA);

    // Third child should have embedding3
    assertEquals("child3", processedChildren.get(2).getId());
    assertEquals(0.5F, processedChildren.get(2).getFloatList("embeddings").get(0), DELTA);
  }

  @Test
  public void testEmbedParentWithMultipleChildren() throws Exception {
    OpenAIEmbed stage = (OpenAIEmbed) StageFactory.of(OpenAIEmbed.class)
        .get("OpenAIEmbedTest/embedChildAndParentDoc.conf");
    EmbeddingModel model = mock(EmbeddingModel.class);

    // Parent embedding
    Embedding parentEmbedding = mock(Embedding.class);
    when(parentEmbedding.vector()).thenReturn(new float[] {1.0F, 2.0F});

    // Child embeddings
    Embedding child1Embedding = mock(Embedding.class);
    when(child1Embedding.vector()).thenReturn(new float[] {3.0F, 4.0F});

    Embedding child2Embedding = mock(Embedding.class);
    when(child2Embedding.vector()).thenReturn(new float[] {5.0F, 6.0F});

    when(model.embedAll(Mockito.any(List.class)))
        .thenReturn(Response.from(List.of(parentEmbedding, child1Embedding, child2Embedding)));
    stage.setModel(model);

    Document parent = Document.create("parent");
    parent.setField("text", "Parent text");

    Document child1 = Document.create("child1");
    child1.setField("text", "Child 1 text");
    parent.addChild(child1);

    Document child2 = Document.create("child2");
    child2.setField("text", "Child 2 text");
    parent.addChild(child2);

    stage.processDocument(parent);

    // Parent should have its embedding
    assertEquals(1.0F, parent.getFloatList("embeddings").get(0), DELTA);

    // Children should maintain order and have their embeddings
    List<Document> processedChildren = parent.getChildren();
    assertEquals(2, processedChildren.size());

    assertEquals("child1", processedChildren.get(0).getId());
    assertEquals(3.0F, processedChildren.get(0).getFloatList("embeddings").get(0), DELTA);

    assertEquals("child2", processedChildren.get(1).getId());
    assertEquals(5.0F, processedChildren.get(1).getFloatList("embeddings").get(0), DELTA);
  }

}
