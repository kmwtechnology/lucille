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
 * Retrieves Text from a Lucille Document Field and breaks them into chunks by placing
 * each chunk as a child Document.
 *
 * Config Parameters:
 * - text_field (String) : field of which Chunking Stage will chunk the text.
 * - character_limit (Integer, optional) : hard limit number of characters in a chunk. Truncate rest. Performed before overlap if
 *   both are set.
 * - new_chunk_size (Integer, optional) : how many chunks to merge into one new Chunk, essentially window size.
*    defaults to 1, keeping the chunks the same.
 * - chunks_to_overlap (Integer, optional) : how many smaller chunks to overlap between merged chunks, essentially setting window stride
 *   defaults to null, which will move window stride by 1
 * - output_name (String, optional): the name of the field that will hold the chunk contents in the children documents. Defaults to "chunk".
 * - separator (String, optional, required if custom chunking): regEx that will be used to split chunks
 * - chunking_method (Type Enum, optional) : how to split contents in text_field. Defaults to Sentence chunking
 *  1. fixed chunking: split by character limit
 *  2. paragraph chunking: split by 2 consecutive \n with optional whitespaces, e.g. \n\n \n \n
 *  3. sentence chunking: use openNLP sentence model for splitting
 *  4. custom chunking: separator option in config required, used as regular expression to split content
 *  5. TODO: maybe semantic chunking? intensive to embed locally
 *    - threshold types: percentile, standard_deviation, interquartile
 *
 *  - child document fields:
 *       - "id" : the child id, in the format of "parent_id-chunk_number"
 *       - "parent_id" : id of parent Document
 *       - "offset" : number of character offset from start of document
 *       - "length" : number of characters in this chunks
 *       - "chunk_number" : chunk number
 *       - "total_chunk_number" : total chunk number produced from parent document
 *       - "chunk" : the chunk contents. field name can be changed with config option "output_name"
 */

public class Chunking extends Stage {

  private final Integer characterLimit;
  private final Integer newChunkSize;
  private Integer chunksToOverlap;
  private final ChunkingMethod method;
  private final String separator;
  private final String outputName;
  private final String text_field;
  private SentenceDetector sentenceDetector;
  private static final Logger log = LogManager.getLogger(Chunking.class);

  public Chunking(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "new_chunk_size", "output_name", "separator", "character_limit",
            "chunks_to_overlap")
        .withRequiredProperties("text_field"));
    characterLimit = config.hasPath("character_limit") ? config.getInt("character_limit") : -1;
    newChunkSize = config.hasPath("new_chunk_size") ? config.getInt("new_chunk_size") : 1;
    chunksToOverlap = config.hasPath("chunks_to_overlap") ? config.getInt("chunks_to_overlap") : null;
    method = ChunkingMethod.fromConfig(config);
    separator = config.hasPath("separator") ? config.getString("separator") : "";
    text_field = config.getString("text_field");
    outputName = config.hasPath("output_name") ? config.getString("output_name") : "chunk";

    if (newChunkSize < 1) {
      throw new StageException("newChunkSize must be greater than 1.");
    }
    if (chunksToOverlap != null && chunksToOverlap >= newChunkSize) {
      throw new StageException("chunksToOverlap must be smaller than newChunkSize.");
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
        // regex pattern for paragraph: find any consecutive line break sequence (\n, \r, \r\n) within one unit of whitespace
        String regex = (method == ChunkingMethod.CUSTOM) ? separator : "\\s*(?>\\R)\\s*(?>\\R)\\s*";
        chunks = content.split(regex);
        break;
      case FIXED:
        // splitting by characterLimit fails to split properly when there is line break sequence (\n, \r, \r\n) in content,
        // so have decided to remove them before
        content = content.replaceAll("(?>\\R)", " ");
        chunks = content.split("(?<=\\G.{" + characterLimit + "})");
        break;
      default: // SENTENCE
        chunks = sentenceDetector.sentDetect(content);
        break;
    }

    // fixed method has already replaced all newline characters with " " before splitting
    if (method != ChunkingMethod.FIXED) {
      // replacing all newline characters in chunks
      for (int i = 0; i < chunks.length; i++) {
        chunks[i] = chunks[i].replaceAll("(?>\\R)", " ");
      }
    }

    // truncating if we have character limit set
    if (method != ChunkingMethod.FIXED && characterLimit > 0) {
      truncateRest(chunks, characterLimit);
    }

    // overlap chunks if we have buffer set
    if (newChunkSize > 1) chunks = overlapChunks(chunks);

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
      for (int j = i; j < Math.min(i + newChunkSize, chunkLength); j++) {
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
    return chunks == null || chunks.length == 0 || newChunkSize > chunks.length;
  }

  // step size is new chunk size - chunksToOverlap. Set default to 1 if no overlap is defined
  private int calculateStepSize() {
    return (chunksToOverlap == null) ? 1 : newChunkSize - chunksToOverlap;
  }

  // if no overlapping of chunks, means step size is 1, and the end index to stop moving window is totalChunksLength - newChunkSize + 1
  // else when overlapping last index MUST contain new information (cannot just be the overlap parts of previous window)
  private int calculateEndIndex(int totalChunksLength) {
    return (chunksToOverlap == null) ? totalChunksLength - newChunkSize + 1 : totalChunksLength - chunksToOverlap;
  }



  private void createChildrenDocsWithChunks(Document doc, String[] chunks) {
    if (chunks == null || chunks.length == 0) {
      return;
    }

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
