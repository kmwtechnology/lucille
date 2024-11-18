package com.kmwllc.lucille.jlama.stage;

import com.github.tjake.jlama.model.AbstractModel;
import com.github.tjake.jlama.model.ModelSupport;
import com.github.tjake.jlama.model.functions.Generator.PoolingType;
import com.github.tjake.jlama.safetensors.DType;
import com.github.tjake.jlama.safetensors.SafeTensorSupport;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Optional;
import org.apache.commons.io.FileUtils;
import org.apache.zookeeper.common.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This stage uses jlama as an inference machine to download and use the embedding model locally, where it would retrieve the input
 * to the model from a source field, and set the outputs to a destination field. Note that this stage only supports models that have
 * safetensor format and BERT architecture.
 *
 * Note that in order to use this stage you will need to set this environment variable below, or attach it to your java run command:
 * export JDK_JAVA_OPTIONS='--add-modules jdk.incubator.vector'
 *  or
 * java ... --add-modules jdk.incubator.vector ...
 *
 * Config Parameters:
 * - source (String) : field of which the embedding Stage will retrieve content from
 * - dest (String, Optional) : name of the field that will store the embeddings, defaults to "embeddings"
 * - pathToStoreModel (String) : Path where the model will be downloaded to and used
 * - model (String, Optional) : the name of the embedding model to use in the format of owner/name, retrieved from HuggingFace
 * - workingMemoryType (String, Optional) : the data type used during runtime for model, impacts accuracy and resource usage.
 * - workingQuantizationType (String, Optional): the data type used during quantization for model, influences the compression and
 *   speed of the model, trading off accuracy for performance.
 * - deleteModelAfter (Boolean, Optional) : whether to delete the model after use, defaults to false
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
  private final String embeddingModel;
  private final String source;
  private final String pathToStoreModel;
  private final String workingMemoryType;
  private final String workingQuantizationType;
  private final String dest;
  private final boolean deleteModelAfter;
  private AbstractModel model;

  private static final Logger log = LoggerFactory.getLogger(JlamaEmbed.class);

  public JlamaEmbed(Config config) {
    super(config, new StageSpec()
        .withRequiredProperties("model", "source", "pathToStoreModel")
        .withOptionalProperties("workingMemoryType", "workingQuantizationType", "dest", "deleteModelAfter"));
    pathToStoreModel = config.getString("pathToStoreModel");
    this.embeddingModel = config.getString("model");
    this.source = config.getString("source");
    this.dest = config.hasPath("dest") ? config.getString("dest") : "embeddings";
    this.workingMemoryType = config.hasPath("workingMemoryType") ? config.getString("workingMemoryType") : "";
    this.workingQuantizationType = config.hasPath("workingQuantizationType") ? config.getString("workingQuantizationType") : "";
    this.deleteModelAfter = config.hasPath("deleteModelAfter") ? config.getBoolean("deleteModelAfter") : false;
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

  @Override
  public void stop() throws StageException {
    if (model != null) {
      model.close();
    }

    if (deleteModelAfter) {
      // using the same process as how Jlama creates the path to store the model
      String[] parts = embeddingModel.split("/");
      if (parts.length != 0 && parts.length <= 2) {
        String owner;
        String name;
        if (parts.length == 1) {
          owner = null;
          name = embeddingModel;
        } else {
          owner = parts[0];
          name = parts[1];
        }
        Path pathOfStoredModel = Paths.get(pathToStoreModel, Optional.ofNullable(owner).orElse("na") + "_" + name);
        try {
          FileUtils.deleteDirectory(pathOfStoredModel.toFile());
        } catch (IOException e) {
          throw new StageException("Could not delete model after use.", e);
        }
      } else {
        log.warn("Could not delete model after use, as the model name is not in the correct format.");
      }
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
      log.warn("doc id: {} does not have {} field or contains null/empty string. Skipping doc...", doc.getId(), source);
      return null;
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