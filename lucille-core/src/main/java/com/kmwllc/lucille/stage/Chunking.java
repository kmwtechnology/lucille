package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ChunkingMethod;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Iterator;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Retrieves text from a Lucille document field, then breaks it into chunks,
 * with each chunk added as a child document attached to the current document.
 *
 * Config Parameters:
 * - source (String) : field of which Chunking Stage will chunk the text.
 * - dest (String, optional): the name of the field that will hold the chunk contents in the children documents.
 *   Defaults to "chunk".
 * - chunking_method (Type Enum, optional) : how to split contents in source. Defaults to Sentence chunking
 *  1. fixed chunking: split by variable lengthToSplit
 *  2. paragraph chunking: split by 2 consecutive line break sequence (\n, \r, \r\n) with optional whitespaces between,
 *     e.g. \n\n \n \n
 *  3. sentence chunking: use openNLP sentence model for splitting
 *  4. custom chunking: regex option in config required, used to split content
 * - regex (String, only for custom chunking): regEx that will be used to split chunks
 * - length_to_split (Integer, only for fixedSizedChunking)
 * - minimum_chunk_length (Integer, optional): filters out chunks of length less than amount before merging and overlapping
 * - chunks_to_merge (Integer, optional) : how many chunks to merge into the final new Chunk before overlapping is taken place.
 *    defaults to 1, keeping the chunks as they were after splitting.
 * - overlap_percentage (Integer, optional) : adds on neighboring chunk's content based on percentage of current chunk, defaults to 0
 * - character_limit (Integer, optional) : hard limit number of characters in a chunk. Truncate rest. Performed after
 *   merging & overlapping if they are set.
 *
 *  - child document fields:
 *       - "id" : the child id, in the format of "parent_id-chunk_number"
 *       - "parent_id" : id of parent Document
 *       - "offset" : number of character offset from start of document
 *       - "length" : number of characters in this chunks
 *       - "chunk_number" : chunk number
 *       - "total_chunk_number" : total chunk number produced from parent document
 *       - "chunk" : the chunk contents. field name can be changed with config option "dest"
 */

public class Chunking extends Stage {

  private final String source;
  private final String dest;
  private final ChunkingMethod method;
  private final String regEx;
  private final Integer lengthToSplit;
  private final Integer minimumChunkLength;
  private final boolean cleanChunks;
  private final Integer chunksToMerge;
  private final Integer overlapPercentage;
  private final Integer characterLimit;
  private SentenceDetector sentenceDetector;
  private static final Logger log = LogManager.getLogger(Chunking.class);

  public Chunking(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "chunks_to_merge", "dest", "regex", "character_limit",
            "clean_chunks", "overlap_percentage", "length_to_split", "minimum_chunk_length")
        .withRequiredProperties("source"));
    characterLimit = config.hasPath("character_limit") ? config.getInt("character_limit") : -1;
    chunksToMerge = config.hasPath("chunks_to_merge") ? config.getInt("chunks_to_merge") : 1;
    method = ChunkingMethod.fromConfig(config);
    regEx = config.hasPath("regex") ? config.getString("regex") : "";
    source = config.getString("source");
    dest = config.hasPath("dest") ? config.getString("dest") : "chunk";
    cleanChunks = config.hasPath("clean_chunks") ? config.getBoolean("clean_chunks") : false;
    overlapPercentage = config.hasPath("overlap_percentage") ? config.getInt("overlap_percentage") : 0;
    lengthToSplit = config.hasPath("length_to_split") && config.getInt("length_to_split") > 0
        ? config.getInt("length_to_split") : null;
    minimumChunkLength = config.hasPath("minimum_chunk_length") && config.getInt("minimum_chunk_length") > 0
        ? config.getInt("minimum_chunk_length") : -1;
    if (chunksToMerge < 1) {
      throw new StageException("Final chunk size must be greater than 1.");
    }
    if (overlapPercentage < 0 || overlapPercentage > 50) {
      throw new StageException("Overlap percentage must be between 0 and 50.");
    }
    if (characterLimit < -1) {
      throw new StageException("Character limit must be a positive integer if set.");
    }
    if (method == ChunkingMethod.CUSTOM && regEx.isEmpty()) {
      throw new StageException("Provide a non empty regex configuration.");
    }
    if (method == ChunkingMethod.FIXED && lengthToSplit == null) {
      throw new StageException("Provide a positive length to split for fixed sized chunking.");
    }
  }

  @Override
  public void start() throws StageException {
    // load sentence model if we are using that chunking method
    if (method == ChunkingMethod.SENTENCE) {
      try (InputStream sentModelIn = getClass().getResourceAsStream("/en-sent.bin")) {
        if (sentModelIn == null) {
          throw new StageException("No sentence model found.");
        }
        SentenceModel sentModel = new SentenceModel(sentModelIn);
        sentenceDetector = new SentenceDetectorME(sentModel);
      } catch (IOException e) {
        throw new StageException("Could not load sentence model.", e);
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // check if document is valid
    if (!doc.hasNonNull(source) || StringUtils.isBlank(doc.getString(source))) {
      log.warn("doc {} does not contain {} field or contains content to chunk, skipping doc...", doc.getId(), source);
      return null;
    }

    // retrieve content to chunk
    String content = doc.getString(source);

    // for testing: log.info("content retrieved {} ", content);
    // splitting up content based on chunking method
    String[] chunks;
    switch (method) {
      case CUSTOM:
        chunks = content.split(regEx);
        break;
      case PARAGRAPH:
        // split any consecutive line break sequence (\n, \r, \r\n) optionally within one unit of whitespace
        chunks = content.split("\\s*(?>\\R)\\s*(?>\\R)\\s*");
        break;
      case FIXED:
        chunks = splitBySize(content, lengthToSplit);
        break;
      default: // SENTENCE
        chunks = sentenceDetector.sentDetect(content);
        break;
    }

    // removing newline characters and trim if clean chunks was selected
    if (cleanChunks) cleanChunks(chunks);

    // filtering chunks by number of characters
    if (minimumChunkLength > 0) chunks = filterChunksByLength(chunks, minimumChunkLength);

    // merge chunks if we have final chunk size set more than 1
    if (chunksToMerge > 1) chunks = mergeChunks(chunks, chunksToMerge);

    // overlap chunks if we have overlap percentage set
    if (overlapPercentage > 0) chunks = overlapChunks(chunks, overlapPercentage);

    // truncating if we have character limit set
    if (characterLimit > 0) truncateRest(chunks, characterLimit);

    // for testing: log.info("number of chunks {} ", chunks.length);

    // creating attached children doc for each chunk
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

  public String[] splitBySize(String input, int chunkSize) {
    if (input == null || chunkSize <= 0) {
      throw new IllegalArgumentException("Input string cannot be null and chunk size must be greater than 0");
    }

    int inputLength = input.length();
    int numOfChunks = (inputLength + chunkSize - 1) / chunkSize;
    String[] chunks = new String[numOfChunks];

    int start = 0;
    for (int i = 0; i < numOfChunks; i++) {
      int end = Math.min(start + chunkSize, inputLength);
      chunks[i] = input.substring(start, end);
      start = end;
    }

    return chunks;
  }

  // replacing all new line characters with white spaces and trim at the end
  private void cleanChunks(String[] chunks) {
    for (int i = 0; i < chunks.length; i++) {
      chunks[i] = chunks[i].replaceAll("(?>\\R)", " ").trim();
    }
  }

  private String[] filterChunksByLength(String[] inputStrings, Integer minimumChunkLength) {
    return Arrays.stream(inputStrings)
        .filter(s -> s != null && s.length() > minimumChunkLength)
        .toArray(String[]::new);
  }

  private String[] mergeChunks(String[] chunks, int chunkSize) {
    int length = chunks.length;
    int resultSize = (length + chunkSize - 1) / chunkSize;
    String[] result = new String[resultSize];
    StringBuilder sb = new StringBuilder();

    for (int i = 0, chunkIndex = 0; i < resultSize; i++) {
      sb.setLength(0);
      int count = 0;
      while (count < chunkSize && chunkIndex < length) {
        if (count > 0) {
          sb.append(" ");
        }
        sb.append(chunks[chunkIndex++]);
        count++;
      }
      result[i] = sb.toString();
    }

    return result;
  }

  private String[] overlapChunks(String[] chunks, Integer overlapPercentage) {
    if (chunks == null || chunks.length <= 1) {
      return chunks;
    }

    String[] result = new String[chunks.length];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < chunks.length; i++) {
      // reset sb
      sb.setLength(0);
      sb.append(chunks[i]);

      // calculate the number of characters to overlap
      int overlapChars = Math.floorDiv(chunks[i].length() * overlapPercentage, 100);

      // Add overlap from the previous chunk
      if (i > 0) {
        String prevChunk = chunks[i - 1];
        int startIndex = Math.max(0, prevChunk.length() - overlapChars); // if the overlap would be longer than prev chunk
        sb.insert(0, prevChunk.substring(startIndex) + " ");
      }

      // Add overlap from the next chunk
      if (i < chunks.length - 1) {
        String nextChunk = chunks[i + 1];
        int endIndex = Math.min(overlapChars, nextChunk.length()); // if the overlap would be longer than next chunk
        sb.append(" ").append(nextChunk, 0, endIndex);
      }

      result[i] = sb.toString().trim();
    }

    return result;
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
      childDoc.setField(dest, chunk);

      doc.addChild(childDoc);
    }
  }
}
