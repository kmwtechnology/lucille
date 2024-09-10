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
    // if used wrong name or another model not by OpenAI, will use text-embedding-3-small as default model, will not throw exception
    assertNotNull(factory.get("OpenAIEmbedTest/wrongModelName.conf"));
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
    // currently the stage would replace the child with another clone child
    // we do not currently support modifying children documents
    List<Document> newProcessedChildrenDocs = parent.getChildren();
    assertEquals(1, newProcessedChildrenDocs.size());

    Document newProcessedChild = parent.getChildren().get(0);
    List<Float> actualEmbedding = newProcessedChild.getFloatList("embeddings");
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(vectors[i], actualEmbedding.get(i), DELTA);
    }
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

    // test that parent has embeddings
    List<Float> actualEmbedding = parent.getFloatList("embeddings");
    for (int i = 0; i < vectors.length; i++) {
      assertEquals(vectors[i], actualEmbedding.get(i), DELTA);
    }

    // currently the stage would replace the child with another clone child
    // we do not currently support modifying children documents
    List<Document> newProcessedChildrenDocs = parent.getChildren();
    assertEquals(1, newProcessedChildrenDocs.size());

    Document newProcessedChild = parent.getChildren().get(0);
    actualEmbedding = newProcessedChild.getFloatList("embeddings");
    for (int i = 0; i < vectors2.length; i++) {
      assertEquals(vectors2[i], actualEmbedding.get(i), DELTA);
    }
  }

}
