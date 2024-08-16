package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ChunkingMethod;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Process of Chunking Stage:
 * - retrieve text information from a Lucille document field
 * - process text to chunks
 * - create child document for each chunk
 *  - child document fields:
 *      - id
 *      - id of parent Document
 *      - offset from start of document (character)
 *      - length (character)
 *      - chunk number
 *      - total chunk number
 *      - chunk itself
 *
 * Things to consider while processing:
 * Token limit (optional) -> hard limit. Truncate rest
 * chunk overlap (optional) -> user may want to overlap sentence to not "lose" information
 * Chunking Methods (Type Enum, default to fixed size)
 * 1. fixed size chunking -> split by character count
 * 2. paragraph chunking -> split by 2 consecutive \n with optional whitespaces
 * 3. Sentence chunking -> use sentence model for splitting
 * 4. custom chunking via RegEx -> separator option in config required
 * 5. TODO: maybe semantic chunking -> very intensive
 *  - threshold types: percentile, standard_deviation, interquartile
 *  -
 */

public class Chunking extends Stage {

  private final Integer characterLimit;
  private final Integer chunkBufferSize;
  private Integer bufferOverlapSize;
  private final ChunkingMethod method;
  private final String separator;
  private final String outputName;
  private final String text_field;
  private SentenceDetector sentenceDetector;
  private static final Logger log = LogManager.getLogger(Chunking.class);

  public Chunking(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "chunk_buffer_size", "output_name", "separator", "character_limit",
            "buffer_overlap_size")
        .withRequiredProperties("text_field"));
    characterLimit = config.hasPath("character_limit") ? config.getInt("character_limit") : -1;
    chunkBufferSize = config.hasPath("chunk_buffer_size") ? config.getInt("chunk_buffer_size") : 1;
    bufferOverlapSize = config.hasPath("buffer_overlap_size") ? config.getInt("buffer_overlap_size") : null;
    method = ChunkingMethod.fromConfig(config);
    separator = config.hasPath("separator") ? config.getString("separator") : "";
    text_field = config.getString("text_field");
    outputName = config.hasPath("output_name") ? config.getString("output_name") : "chunk";

    if (chunkBufferSize < 1) {
      throw new StageException("chunkBufferSize must be greater than 1.");
    }
    if (bufferOverlapSize != null && bufferOverlapSize > chunkBufferSize) {
      throw new StageException("bufferOverlapSize must be smaller than chunkBufferSize.");
    }
    if (characterLimit < -1) {
      throw new StageException("Character limit must be a positive integer");
    }
    if (text_field == null) {
      throw new StageException("text_field configuration where Chunk Stage would take as input is missing");
    }
    if (method == ChunkingMethod.CUSTOM && separator.isEmpty()) {
      throw new StageException("Provide a non empty regEx for separator configuration");
    }
    if (method == ChunkingMethod.FIXED && characterLimit <= 0) {
      throw new StageException("Provide a positive character_limit for fixed sized chunking");
    }
  }

  @Override
  public void start() throws StageException {
    // load sentence model if we are using that chunking method
    if (method == ChunkingMethod.SENTENCE) {
      try (InputStream sentModelIn = getClass().getResourceAsStream("/en-sent.bin")) {
        if (sentModelIn == null) {
          throw new StageException("No sentence model found");
        }
        SentenceModel sentModel = new SentenceModel(sentModelIn);
        sentenceDetector = new SentenceDetectorME(sentModel);
      } catch (IOException e) {
        throw new StageException("Could not load sentence model", e);
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // retrieve content to chunk
    String content = doc.getString(text_field);
    // for testing: log.info("content retrieved {} ", content);

    // splitting up content based on chunking method
    String[] chunks;
    switch (method) {
      case CUSTOM:
      case PARAGRAPH:
        // regex pattern for paragraph: find any consecutive newline characters within one unit of whitespace
        String regex = (method == ChunkingMethod.CUSTOM) ? separator : "\\s*(?>\\R)\\s*(?>\\R)\\s*";
        chunks = content.split(regex);

        break;
      case FIXED:
        // splitting by characterLimit does not work when there is newline characters in content so need to remove them before
        content = content.replaceAll("(?>\\R)", " ");
        chunks = content.split("(?<=\\G.{" + characterLimit + "})");
        break;
      default: // SENTENCE
        chunks = sentenceDetector.sentDetect(content);
        break;
    }

    // replacing all newline characters in chunks
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = chunks[i].replaceAll("(?>\\R)", " ");
    }

    // overlap chunks if we have buffer set
    if (chunkBufferSize > 1) chunks = overlapChunks(chunks);

    // truncating if we have character limit set
    if (method != ChunkingMethod.FIXED && characterLimit > 0) {
      truncateRest(chunks, characterLimit);
    }

    // for testing: log.info("number of chunks {} ", chunks.length);

    // create children doc for each chunks
    createChildrenDocsWithChunks(doc, chunks);

    return null;
  }

  private void truncateRest(String[] chunks, int characterLimit) {
    for (int i = 0; i < chunks.length; i++) {
      String s = chunks[i];
      if (s.length() > characterLimit) {
        chunks[i] = s.substring(0, characterLimit);
      }
    }
  }

  private String[] overlapChunks(String[] chunks) {
    // invalid if chunks is null/empty, or lesser than chunkBufferSize, no point overlapping
    if (isInvalidInput(chunks)) {
      return chunks;
    }

    int chunkLength = chunks.length;
    int stepSize = calculateStepSize();
    int endIndex = calculateEndIndex(chunkLength);
    int resultSize = calculateResultSize(stepSize, endIndex);

    String[] resultChunks = new String[resultSize];
    StringBuilder sb = new StringBuilder();

    // go through each window and merge them
    for (int i = 0, resultIndex = 0; i < endIndex; i += stepSize, resultIndex++) {
      sb.setLength(0);
      for (int j = i; j < Math.min(i + chunkBufferSize, chunkLength); j++) {
        sb.append(chunks[j]).append(" ");
      }
      // for testing: log.info("{} {} {}", i, resultIndex, sb.toString());
      resultChunks[resultIndex] = sb.toString().trim();
    }

    return resultChunks;
  }

  private int calculateResultSize(int stepSize, int endIndex) {
    return (endIndex - 1) / stepSize + 1;
  }

  // if chunks is null, empty or the chunkBufferSize is larger than the length then skip
  private boolean isInvalidInput(String[] chunks) {
    return chunks == null || chunks.length == 0 || chunkBufferSize > chunks.length;
  }

  private int calculateStepSize() {
    return (bufferOverlapSize == null) ? 1 : Math.max(1, chunkBufferSize - bufferOverlapSize);
  }

  private int calculateEndIndex(int chunkLength) {
    return (bufferOverlapSize == null) ? chunkLength - chunkBufferSize + 1 : chunkLength - bufferOverlapSize;
  }



  private void createChildrenDocsWithChunks(Document doc, String[] chunks) {
    if (chunks == null || chunks.length == 0) {
      return;
    }
    // get the id of the parent
    String parentId = doc.getString(Document.ID_FIELD);
    int totalChunks = chunks.length;
    int offset = 0;

    for (int i = 0; i < totalChunks; i++) {
      // currently set children id to be parentsId-chunkNumber
      String id = parentId + "-" + (i + 1);
      Document childDoc = Document.create(id);

      childDoc.setField("parent_id", parentId);
      childDoc.setField("chunk_number", i + 1);
      childDoc.setField("total_chunks", totalChunks);

      String chunk = chunks[i];
      int length = chunk.length();

      childDoc.setField("offset", offset);
      offset += length;
      childDoc.setField("length", length);
      childDoc.setField(outputName, chunk);

      doc.addChild(childDoc);
    }
  }
}
