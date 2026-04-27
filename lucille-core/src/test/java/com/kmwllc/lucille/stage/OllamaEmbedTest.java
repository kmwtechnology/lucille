package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import io.github.ollama4j.OllamaAPI;
import io.github.ollama4j.models.embeddings.OllamaEmbedResponseModel;
import java.util.List;
import org.junit.Test;
import org.mockito.MockedConstruction;

public class OllamaEmbedTest {

  private final StageFactory factory = StageFactory.of(OllamaEmbed.class);

  private static final float DELTA = 1e-6f;

  @Test
  public void testBasicEmbedding() throws Exception {
    OllamaEmbedResponseModel response = new OllamaEmbedResponseModel();
    response.setEmbeddings(List.of(List.of(0.1, 0.2, 0.3, 0.4, 0.5)));

    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class, (api, context) -> {
      when(api.embed(any())).thenReturn(response);
    })) {
      Stage stage = factory.get("OllamaEmbedTest/basic.conf");

      Document doc = Document.create("doc1");
      doc.setField("text", "HVAC maintenance services for government buildings");

      stage.processDocument(doc);

      assertTrue(doc.has("embeddings"));
      List<Float> embeddings = doc.getFloatList("embeddings");
      assertEquals(5, embeddings.size());
      assertEquals(0.1f, embeddings.get(0), DELTA);
      assertEquals(0.5f, embeddings.get(4), DELTA);
    }
  }

  @Test
  public void testCustomDestField() throws Exception {
    OllamaEmbedResponseModel response = new OllamaEmbedResponseModel();
    response.setEmbeddings(List.of(List.of(0.1, 0.2, 0.3)));

    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class, (api, context) -> {
      when(api.embed(any())).thenReturn(response);
    })) {
      Stage stage = factory.get("OllamaEmbedTest/customDest.conf");

      Document doc = Document.create("doc1");
      doc.setField("text", "Test content");

      stage.processDocument(doc);

      assertFalse(doc.has("embeddings"));
      assertTrue(doc.has("my_embeddings"));
      assertEquals(3, doc.getFloatList("my_embeddings").size());
    }
  }

  @Test
  public void testSkipBlankSource() throws Exception {
    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class)) {
      Stage stage = factory.get("OllamaEmbedTest/basic.conf");

      // No source field at all
      Document doc1 = Document.create("doc1");
      stage.processDocument(doc1);
      assertFalse(doc1.has("embeddings"));

      // Empty source field
      Document doc2 = Document.create("doc2");
      doc2.setField("text", "");
      stage.processDocument(doc2);
      assertFalse(doc2.has("embeddings"));

      // Blank source field
      Document doc3 = Document.create("doc3");
      doc3.setField("text", "   ");
      stage.processDocument(doc3);
      assertFalse(doc3.has("embeddings"));
    }
  }

  @Test
  public void testTimeoutConfiguration() throws Exception {
    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class)) {
      Stage stage = factory.get("OllamaEmbedTest/withTimeout.conf");

      OllamaAPI constructedAPI = mockAPI.constructed().get(0);
      verify(constructedAPI).setRequestTimeoutSeconds(30L);
    }
  }

  @Test
  public void testOllamaServerError() throws Exception {
    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class, (api, context) -> {
      when(api.embed(any())).thenThrow(new RuntimeException("Connection refused"));
    })) {
      Stage stage = factory.get("OllamaEmbedTest/basic.conf");

      Document doc = Document.create("doc1");
      doc.setField("text", "This should fail");

      assertThrows(StageException.class, () -> stage.processDocument(doc));
    }
  }

  @Test
  public void testEmptyEmbeddingResponse() throws Exception {
    OllamaEmbedResponseModel response = new OllamaEmbedResponseModel();
    response.setEmbeddings(List.of());

    try (MockedConstruction<OllamaAPI> mockAPI = mockConstruction(OllamaAPI.class, (api, context) -> {
      when(api.embed(any())).thenReturn(response);
    })) {
      Stage stage = factory.get("OllamaEmbedTest/basic.conf");

      Document doc = Document.create("doc1");
      doc.setField("text", "This returns empty embeddings");

      stage.processDocument(doc);

      assertFalse(doc.has("embeddings"));
    }
  }
}
