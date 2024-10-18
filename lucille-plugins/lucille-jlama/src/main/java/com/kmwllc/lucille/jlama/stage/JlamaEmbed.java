package com.kmwllc.lucille.jlama.stage;

import com.github.tjake.jlama.model.AbstractModel;
import com.github.tjake.jlama.model.ModelSupport;
import com.github.tjake.jlama.model.functions.Generator.PoolingType;
import com.github.tjake.jlama.safetensors.DType;
import com.github.tjake.jlama.safetensors.SafeTensorSupport;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.stage.OpenAIEmbed;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage uses jlama as an inference machine to download and use the embedding model locally, where it would retrieve the input
 * to the model from a source field, and set the outputs to a destination field. This stage is for Java 20 and above.
 * Note that in order to use this stage you might need to set this environment variable below, or attach it to your java run command:
 * JDK_JAVA_OPTIONS=--add-modules jdk.incubator.vector
 *
 * Config Parameters:
 * - source (String) : field of which the embedding Stage will retrieve content from
 * - dest (String, Optional) : name of the field that will hold the embeddings, defaults to "embeddings"
 * - localModelFolderPath (String) : Path where the model will be downloaded to and used
 * - model (String, Optional) : the name of the embedding model to use in the format of owner/name
 * - workingMemoryType (String, Optional) : the data type used during runtime for model, impacts accuracy and resource usage.
 * - workingQuantizationType (String, Optional): the data type used during quantization for model, influences the compression and
 *   speed of the model, trading off accuracy for performance.
 *
 *   DType Options:
 *    "BOOL" (1 byte)
 *    "U8" (Unsigned 8-bit integer)
 *    "I8" (Signed 8-bit integer)
 *    "I16" (Signed 16-bit integer)
 *    "U16" (Unsigned 16-bit integer)
 *    "F16" (16-bit floating point)
 *    "BF16" (BFloat16, 16-bit floating point)
 *    "I32" (Signed 32-bit integer)
 *    "U32" (Unsigned 32-bit integer)
 *    "F32" (32-bit floating point)
 *    "F64" (64-bit floating point)
 *    "I64" (Signed 64-bit integer)
 *    "U64" (Unsigned 64-bit integer)
 *    "Q4" (Quantized 4-bit integer)
 *    "Q5" (Quantized 5-bit integer)
 */

public class JlamaEmbed extends Stage {
  public final String embeddingModel;
  public final String source;
  public final String pathToStoreModel;
  public final String workingMemoryType;
  public final String workingQuantizationType;
  public final String dest;
  public AbstractModel model;

  private static final Logger log = LoggerFactory.getLogger(OpenAIEmbed.class);

  public JlamaEmbed(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("model", "source", "pathToStoreModel")
        .withOptionalProperties("workingMemoryType", "workingQuantizationType"));
    pathToStoreModel = config.getString("pathToStoreModel");
    this.embeddingModel = config.getString("model");
    this.source = config.getString("source");
    this.dest = config.hasPath("dest") ? config.getString("dest") : "embeddings";
    this.workingMemoryType = config.hasPath("workingMemoryType") ? config.getString("workingMemoryType") : "";
    this.workingQuantizationType = config.hasPath("workingQuantizationType") ? config.getString("workingQuantizationType") : "";
  }

  @Override
  public void start() throws StageException {
    try {
      File localModelPath = SafeTensorSupport.maybeDownloadModel(pathToStoreModel, embeddingModel);
      model = ModelSupport.loadEmbeddingModel(localModelPath,
          getDType(workingMemoryType, DType.F32),
          getDType(workingQuantizationType, DType.I8));
    } catch (IllegalArgumentException e) {
      throw new StageException("Model must be in the form owner/name or could not find necessary files.", e);
    } catch (IOException e) {
      throw new StageException("Could not download embedding model.", e);
    } catch (RuntimeException e) {
      throw new StageException("Error loading model.", e);
    } catch (Exception e) {
      throw new StageException("Something went wrong while downloading/loading model.", e);
    }
  }

  private DType getDType(String type, DType defaultType) {
    return switch (type.trim().toUpperCase()) {
      case "BOOL" -> DType.BOOL;
      case "U8" -> DType.U8;
      case "I8" -> DType.I8;
      case "I16" -> DType.I16;
      case "U16" -> DType.U16;
      case "F16" -> DType.F16;
      case "BF16" -> DType.BF16;
      case "I32" -> DType.I32;
      case "U32" -> DType.U32;
      case "F32" -> DType.F32;
      case "F64" -> DType.F64;
      case "I64" -> DType.I64;
      case "U64" -> DType.U64;
      case "Q4" -> DType.Q4;
      case "Q5" -> DType.Q5;
      default -> defaultType;
    };
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source) || StringUtils.isBlank(doc.getString(source))) {
      throw new StageException("doc id: " + doc.getId() + " does not have " + source + " field or contains null/empty string.");
    }

    String toEmbed = doc.getString(source);
    try {
      float[] embeddings = model.embed(toEmbed, PoolingType.MODEL);
      for (float embedding : embeddings) {
        doc.setOrAdd(dest, embedding);
      }
    } catch (Exception e) {
      throw new StageException("Error embedding document: " + doc.getId(), e);
    }

    return null;
  }
}