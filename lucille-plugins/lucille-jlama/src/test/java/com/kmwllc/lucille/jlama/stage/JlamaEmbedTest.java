package com.kmwllc.lucille.jlama.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

import com.github.tjake.jlama.model.AbstractModel;
import com.github.tjake.jlama.model.ModelSupport;
import com.github.tjake.jlama.safetensors.DType;
import com.github.tjake.jlama.safetensors.SafeTensorSupport;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import com.kmwllc.lucille.stage.StageFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;

public class JlamaEmbedTest {

  private StageFactory factory = StageFactory.of(JlamaEmbed.class);
  public Config defaultConfig;

  @Before
  public void setUp() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("source", "source");
    configValues.put("pathToStoreModel", "./model");
    configValues.put("model", "owner/name");
    defaultConfig = ConfigFactory.parseMap(configValues);
  }

  @Test
  public void testGettingDType() throws StageException {
    // create the test config, here DTypes is set to F16 and I16
    HashMap<String, Object> configValues2 = new HashMap<>();
    configValues2.put("source", "source");
    configValues2.put("pathToStoreModel", "./model");
    configValues2.put("model", "owner/name");
    configValues2.put("workingMemoryType", "F16");
    configValues2.put("workingQuantizationType", "I16");
    Config otherConfig = ConfigFactory.parseMap(configValues2);

    // create the test config, here DTypes is set to U16 and Q5
    HashMap<String, Object> configValues3 = new HashMap<>();
    configValues3.put("source", "source");
    configValues3.put("pathToStoreModel", "./model");
    configValues3.put("model", "owner/name");
    configValues3.put("workingMemoryType", "U16");
    configValues3.put("workingQuantizationType", "Q5");
    Config thirdConfig = ConfigFactory.parseMap(configValues3);

    File mockFile = mock(File.class);
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class);
        MockedStatic<ModelSupport> mockModelSupport = mockStatic(ModelSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mockFile);
      mockModelSupport.when(() -> ModelSupport.loadEmbeddingModel(any(), any(), any())).thenReturn(mock(AbstractModel.class));

      // factory.get calls stage.start()
      Stage stage = factory.get(defaultConfig);
      Stage stage2 = factory.get(otherConfig);
      Stage stage3 = factory.get(thirdConfig);

      // capture DTypes
      ArgumentCaptor<DType> argumentCaptor2 = ArgumentCaptor.forClass(DType.class);
      ArgumentCaptor<DType> argumentCaptor3 = ArgumentCaptor.forClass(DType.class);

      // verify that loadEmbeddingModel is called twice one for defaultConfig one for otherConfig
      mockModelSupport.verify(() -> ModelSupport.loadEmbeddingModel(any(), argumentCaptor2.capture(), argumentCaptor3.capture()), times(3));

      // first stage
      assertEquals(DType.F32, argumentCaptor2.getAllValues().get(0));
      assertEquals(DType.I8, argumentCaptor3.getAllValues().get(0));
      // second stage
      assertEquals(DType.F16, argumentCaptor2.getAllValues().get(1));
      assertEquals(DType.I16, argumentCaptor3.getAllValues().get(1));
      // third stage
      assertEquals(DType.U16, argumentCaptor2.getAllValues().get(2));
      assertEquals(DType.Q5, argumentCaptor3.getAllValues().get(2));
    }
  }

  @Test
  public void testInvalidStart() throws StageException {
    HashMap<String, Object> configValues2 = new HashMap<>();
    configValues2.put("source", "source");
    configValues2.put("pathToStoreModel", "./model");
    configValues2.put("model", "ownerName");
    Config modelNameNotInFormat = ConfigFactory.parseMap(configValues2);

    // test model not in format of owner/name
    try (MockedStatic<ModelSupport> mockModelSupport = mockStatic(ModelSupport.class)) {
      mockModelSupport.when(() -> ModelSupport.loadEmbeddingModel(any(), any(), any())).thenReturn(mock(AbstractModel.class));
      assertThrows(StageException.class, () -> factory.get(modelNameNotInFormat));
    }

    // testing fail IOException requires missing model from huggingface, so just skipping this test

    // testing if given a path that does not contain model files (folder does not exist)
    File mockFile = mock(File.class);
    when(mockFile.exists()).thenReturn(false).thenReturn(true);
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mockFile);
      assertThrows(StageException.class, () -> factory.get(defaultConfig));
    }
    // testing when file exists but is not a directory
    when(mockFile.isFile()).thenReturn(true).thenReturn(true);
    when(mockFile.isDirectory()).thenReturn(false).thenReturn(true);
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mockFile);
      assertThrows(StageException.class, () -> factory.get(defaultConfig));
    }

    // testing when file exists, is directory but contain no other files that should be expected
    when(mockFile.listFiles()).thenReturn(new File[]{mock(File.class)});
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mockFile);
      assertThrows(StageException.class, () -> factory.get(defaultConfig));
    }
  }

  @Test
  public void testMissingOrInvalidSourceField() throws StageException {
    AbstractModel mockModel = mock(AbstractModel.class);
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class);
        MockedStatic<ModelSupport> mockModelSupport = mockStatic(ModelSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mock(File.class));
      mockModelSupport.when(() -> ModelSupport.loadEmbeddingModel(any(), any(), any())).thenReturn(mockModel);
      // embedding process should not fail
      when(mockModel.embed(any(), any())).thenReturn(new float[]{0.1F, 0.2F, 0.3F});

      Stage stage = factory.get(defaultConfig);
      Document doc = Document.create("doc1");
      Document doc2 = Document.create("doc2");
      doc2.setField("source", (String) null);
      Document doc3 = Document.create("doc3");
      doc3.setField("source", "");

      // fail due to no source field or value of source field is null/empty
      assertThrows(StageException.class, () -> stage.processDocument(doc));
      assertThrows(StageException.class, () -> stage.processDocument(doc2));
      assertThrows(StageException.class, () -> stage.processDocument(doc3));
    }
  }

  @Test
  public void testEmbedding() throws StageException {
    AbstractModel mockModel = mock(AbstractModel.class);
    try (MockedStatic<SafeTensorSupport> mockSafeTensorSupport = mockStatic(SafeTensorSupport.class);
        MockedStatic<ModelSupport> mockModelSupport = mockStatic(ModelSupport.class)) {
      mockSafeTensorSupport.when(() -> SafeTensorSupport.maybeDownloadModel(any(), any())).thenReturn(mock(File.class));
      mockModelSupport.when(() -> ModelSupport.loadEmbeddingModel(any(), any(), any())).thenReturn(mockModel);
      // embeddings
      when(mockModel.embed(any(), any())).thenReturn(new float[]{0.1F, 0.2F, 0.3F})
      .thenReturn(new float[]{0.4F, 0.5F, 0.6F});

      Stage stage = factory.get(defaultConfig);
      Document doc = Document.create("doc1");
      Document doc2 = Document.create("doc2");
      doc.setField("source", "Hello");
      doc2.setField("source", "World");

      stage.processDocument(doc);
      stage.processDocument(doc2);

      List<Float> expectedEmbeddingForDoc = new ArrayList<>(Arrays.asList(0.1F, 0.2F, 0.3F));
      List<Float> expectedEmbeddingForDoc2 = new ArrayList<>(Arrays.asList(0.4F, 0.5F, 0.6F));
      assertEquals(expectedEmbeddingForDoc, doc.getFloatList("embeddings"));
      assertEquals(expectedEmbeddingForDoc2, doc2.getFloatList("embeddings"));
    }
  }
}