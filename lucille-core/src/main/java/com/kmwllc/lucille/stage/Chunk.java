package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ChunkingMethod;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.ModelType;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Consider:
 * Token Limit (required)
 * Chunking Methods: (default to fixed size chunking)
 * 1. fixed size chunking
 * 2. paragraph chunking
 * 3. custom chunking via RegEx -> separator option required
 * Which model tokenizer -> for accurate measurements of tokens
 *
 */

public class Chunk extends Stage {

  private final Integer tokenLimit;
  private final ChunkingMethod method;
  private final String separator;
  private final String outputName;
  private List<String> output;
  private final String text_field;
  private SentenceDetectorME sentenceDetector;
  private Encoding enc;
  private static final Logger log = LogManager.getLogger(Chunk.class);

  public Chunk(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "separator", "pref_token_size", "output_name")
        .withRequiredProperties("token_limit", "text_field", "model_type"));
    tokenLimit = config.getInt("token_limit");
    separator = config.hasPath("separator") ? config.getString("separator") : "";
    method = ChunkingMethod.fromConfig(config);
    text_field = config.getString("text_field");
    outputName = config.hasPath("output_name") ? config.getString("output_name") : "chunks";
    if (tokenLimit <= 0) {
      throw new StageException("Provide a positive token_limit");
    }
    if (text_field == null) {
      throw new StageException("text_field configuration where Chunk Stage would take as input is missing");
    }
    if (method == ChunkingMethod.CUSTOM && separator.isEmpty()) {
      throw new StageException("Provide a non empty regEx for separator configuration");
    }
    String modelTypeStr = config.hasPath("model_type") ? config.getString("model_type") : "text-embedding-3-large";
    ModelType modelType = ModelType.fromName(modelTypeStr).orElse(ModelType.TEXT_EMBEDDING_3_LARGE);
    enc = Encodings.newDefaultEncodingRegistry().getEncodingForModel(modelType);
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
      // load the sentence detector model
      try (InputStream sentModelIn = getClass().getResourceAsStream("/en-sent.bin")) {
        if (sentModelIn == null) {
          throw new StageException("Model file not found");
        }
        SentenceModel sentModel = new SentenceModel(sentModelIn);
        sentenceDetector = new SentenceDetectorME(sentModel);
      } catch (IOException e) {
        throw new StageException("Could not load sentence model", e);
      }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // get content and prepare output
    String content = doc.getString(text_field);
    output = new ArrayList<>();
    // setting context
    String[] chunks;
    switch (method) {
      case CUSTOM:
        chunks = content.split(separator);
        break;
      case PARAGRAPH:
        chunks = content.split("\\n{2,}");
        break;
      default:
        chunks = new String[]{content};
        break;
    }
    // process chunks
    for (String chunk : chunks) {
      if (!chunk.isEmpty()) {
        output.addAll(processChunk(chunk.trim(), tokenLimit, enc, sentenceDetector));
      }
    }

    // convert output to a JsonNode and put into document
    ObjectMapper mapper = new ObjectMapper();
    JsonNode jsonNode = mapper.valueToTree(output);
    doc.setField(outputName, jsonNode);

    return null;
  }

  // processChunk prioritizes token limit but tries to keep sentences together using sentenceDetect (for better context)
  private static List<String> processChunk(String chunk, Integer tokenLimit, Encoding enc,
      SentenceDetector sentenceDetector) {
    List<String> result = new ArrayList<>();

    String[] sentences = sentenceDetector.sentDetect(chunk);
    StringBuilder currentProcessingChunk = new StringBuilder();
    int currentTokenCount = 0;

    for (String sentence : sentences) {
      //convert sentence to tokens
      int sentenceTokenCount = enc.countTokens(sentence);
      // for testing: log.info("sentence {} has token count {}", sentence, sentenceTokenCount);
      // edge case where one sentence is over token limit
      if (sentenceTokenCount > tokenLimit) {
        // handle sentences longer than token limit
        if (currentProcessingChunk.length() > 0) {
          result.add(currentProcessingChunk.toString().trim());
          // setting StringBuilder length to 0 is more efficient than reallocating space for new StringBuilder
          currentProcessingChunk.setLength(0);
          currentTokenCount = 0;
        }
        result.addAll(splitLongSentence(sentence, tokenLimit, enc));
      } else if (currentTokenCount + sentenceTokenCount > tokenLimit) { // if adding sentence would cause over token limit
        result.add(currentProcessingChunk.toString().trim());

        //resetting the chunk
        currentProcessingChunk.setLength(0);

        // continue with new chunk
        currentProcessingChunk.append(sentence).append(" ");
        currentTokenCount = sentenceTokenCount;
      } else {
        currentProcessingChunk.append(sentence).append(" ");
        currentTokenCount += sentenceTokenCount;
      }
    }

    // add any remaining content as a different chunk
    if (currentProcessingChunk.length() > 0) {
      result.add(currentProcessingChunk.toString().trim());
    }
    return result;
  }

  // splits up sentences into words, by using a whitespace regEx
  public static List<String> splitLongSentence(String sentence, Integer tokenLimit, Encoding enc) {
    List<String> result = new ArrayList<>();
    String[] words = sentence.split("\\s+");
    StringBuilder currentChunk = new StringBuilder();
    int currentTokenCount = 0;

    for (String word : words) {
      int wordLength = enc.countTokens(word);
      if (currentTokenCount + wordLength > tokenLimit) {
        if (currentChunk.length() > 0) {
          result.add(currentChunk.toString().trim());
          currentChunk.setLength(0);
          currentTokenCount = 0;
        }
        if (wordLength > tokenLimit) {
          // Handle extremely long words by splitting them
          result.addAll(splitLongWord(word, tokenLimit));
        } else {
          currentChunk.append(word).append(" ");
          currentTokenCount = wordLength;
        }
      } else {
        currentChunk.append(word).append(" ");
        currentTokenCount += wordLength;
      }
    }

    if (currentChunk.length() > 0) {
      result.add(currentChunk.toString().trim());
    }

    return result;
  }

  // handle edge case where words can take more than one token
  public static List<String> splitLongWord(String word, Integer tokenLimit) {
    List<String> result = new ArrayList<>();
    int chunkSize = tokenLimit - 1; // Leave room for a hyphen
    for (int i = 0; i < word.length(); i += chunkSize) {
      int end = Math.min(i + chunkSize, word.length());
      if (end < word.length()) {
        result.add(word.substring(i, end) + "-");
      } else {
        result.add(word.substring(i, end));
      }
    }
    return result;
  }
}
