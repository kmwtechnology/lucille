package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.stage.util.ChunkingMethod;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import opennlp.tools.sentdetect.SentenceDetector;
import opennlp.tools.sentdetect.SentenceDetectorME;
import opennlp.tools.sentdetect.SentenceModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * NOTE: This stage produces documents with ATTACHED children containing the chunks. To have children documents emitted as separate
 * documents, use the emitNestedChildren stage. An example of this can be seen below.
 *
 * Retrieves text from a Lucille document field, then breaks it into chunks, with each chunk added as a child document attached to
 * the current document.
 * Order of processing chunks: chunking method -> cleaning -> pre-merge processing -> merge -> overlap -> character limiting
 *
 * Config Parameters:
 * - source (String) : field of which Chunking Stage will chunk the text.
 * - dest (String, optional): the name of the field that will hold the chunk contents in the children documents.
 *   Defaults to "chunk".
 * - chunking_method (Type Enum, optional) : how to split contents in source. Defaults to Sentence chunking
 *  1. fixed chunking ("fixed"): split by variable lengthToSplit
 *  2. paragraph chunking ("paragraph"): split by 2 consecutive line break sequence (\n, \r, \r\n) with optional whitespaces between,
 *     e.g. \n\n \n \n
 *  3. sentence chunking ("sentence"): use openNLP sentence model for splitting
 *  4. custom chunking ("custom"): regex option in config required, used to split content
 * - regex (String, only for custom chunking): regEx that will be used to split chunks
 * - length_to_split (Integer, only for fixed chunking): length of characters of each initial chunk before processing
 * - pre_merge_min_chunk_len (Integer, optional): removes and append chunk to the neighboring chunk if below given number of characters,
 *    defaults appending to next chunk.
 * - pre_merge_max_chunk_len (Integer, optional): truncates the chunks if over given amount, applies before merging and overlapping
 * - chunks_to_merge (Integer, optional) : how many chunks to merge into the final new Chunk before overlapping is taken place.
 *    defaults to 1, keeping the chunks as they were after splitting.
 *    e.g. chunks_to_merge: 2 -> { chunk1/chunk2, chunk3/chunk4, chunk5/chunk6}
 * - overlap_percentage (Integer, optional) : adds on neighboring chunk's content based on percentage of current chunk, defaults to 0
 * - chunks_to_overlap (Integer, optional) : indicate the number of overlap of smaller chunks to overlap while merging into final chunk
 *    e.g. chunks_to_overlap: 1 -> { chunk1/chunk2/chunk3, chunk3/chunk4/chunk5, chunk5/chunk6/chunk7}
 *         chunks_to_overlap: 2 -> { chunk1/chunk2/chunk3, chunk2/chunk3/chunk4, chunk3/chunk4/chunk5}
 * - character_limit (Integer, optional) : hard limit number of characters in the final chunk. Truncate rest. Performed after
 *   merging and overlapping if they are set.
 *
 *  - child document fields:
 *       - "id" : the child id, in the format of "parent_id-chunk_number"
 *       - "parent_id" : id of parent Document
 *       - "offset" : number of character offset from start of document
 *       - "length" : number of characters in this chunk
 *       - "chunk_number" : chunk number
 *       - "total_chunks" : total chunk number produced from parent document
 *       - "chunk" : the chunk contents. field name can be changed with config option "dest"
 *
 *  e.g. of paragraph chunking configuration, with a minimum size of 50 characters per chunk, followed by emitting the children
 *       documents
 *  {
 *   class: "com.kmwllc.lucille.stage.ChunkText"
 *   source: "text"
 *   chunking_method: "paragraph"
 *   pre_merge_min_chunk_len: 50
 *   clean_chunks: true
 *  },
 *  {
 *    class: "com.kmwllc.lucille.stage.EmitNestedChildren"
 *    drop_parent : true # drop parent document if you do not want it to be indexed
 *  }
 *
 *  e.g. of sentence chunking configuration with 5 sentences per chunk and 1 sentence of overlap, with a limit of 2000 characters
 *  {
 *   source: "text"
 *   chunking_method: "sentence"
 *   chunks_to_merge: 5
 *   chunks_to_overlap: 1
 *   clean_chunks: true
 *   character_limit: 2000
 *  }
 */

public class ChunkText extends Stage {

  private final String source;
  private final String dest;
  private final ChunkingMethod method;
  private final String regEx;
  private final Integer lengthToSplit;
  private final Integer preMergeMinChunkLen;
  private final Integer preMergeMaxChunkLen;
  private final boolean cleanChunks;
  private final Integer chunksToMerge;
  private final Integer chunksToOverlap;
  private final Integer overlapPercentage;
  private final Integer characterLimit;
  private SentenceDetector sentenceDetector;
  private static final Logger log = LogManager.getLogger(ChunkText.class);

  public ChunkText(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "chunks_to_merge", "dest", "regex", "character_limit",
            "clean_chunks", "overlap_percentage", "length_to_split", "pre_merge_min_chunk_len", "pre_merge_max_chunk_len",
            "chunks_to_overlap")
        .withRequiredProperties("source"));
    this.source = config.getString("source");
    this.dest = config.hasPath("dest") ? config.getString("dest") : "chunk";
    this.method = ChunkingMethod.fromConfig(config);
    this.regEx = config.hasPath("regex") ? config.getString("regex") : "";
    this.lengthToSplit = config.hasPath("length_to_split") && config.getInt("length_to_split") > 0
        ? config.getInt("length_to_split") : null;
    this.cleanChunks = config.hasPath("clean_chunks") ? config.getBoolean("clean_chunks") : false;
    this.preMergeMinChunkLen = config.hasPath("pre_merge_min_chunk_len") && config.getInt("pre_merge_min_chunk_len") > 0
        ? config.getInt("pre_merge_min_chunk_len") : -1;
    this.preMergeMaxChunkLen = config.hasPath("pre_merge_max_chunk_len") && config.getInt("pre_merge_max_chunk_len") > 0
        ? config.getInt("pre_merge_max_chunk_len") : -1;
    this.chunksToMerge = config.hasPath("chunks_to_merge") ? config.getInt("chunks_to_merge") : 1;
    this.chunksToOverlap = config.hasPath("chunks_to_overlap") ? config.getInt("chunks_to_overlap") : null;
    this.overlapPercentage = config.hasPath("overlap_percentage") ? config.getInt("overlap_percentage") : 0;
    this.characterLimit = config.hasPath("character_limit") ? config.getInt("character_limit") : -1;
    if (chunksToMerge < 1) {
      throw new StageException("Chunks to merge configuration must be greater than 1 if merging chunks is desired or equal to 1 if undesired.");
    }
    if (characterLimit < -1) {
      throw new StageException("Character limit must be a positive integer if set.");
    }
    if (method == ChunkingMethod.CUSTOM && regEx.isEmpty()) {
      throw new StageException("Must provide a non empty regex configuration when using 'custom' method.");
    }
    if (method == ChunkingMethod.FIXED && lengthToSplit == null) {
      throw new StageException("Provide a positive length to split for fixed sized chunking.");
    }
    if (chunksToOverlap != null && overlapPercentage > 0) {
      throw new StageException("Both chunksToOverlap and overlapPercentage cannot be used. Choose one overlap option.");
    }
    if (chunksToOverlap != null && chunksToOverlap >= chunksToMerge) {
      throw new StageException("Chunks to overlap must be smaller than the chunks to merge.");
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
      log.debug("doc {} does not contain {} field or content to chunk, skipping doc...", doc.getId(), source);
      return null;
    }

    // retrieve content to chunk
    String content = doc.getString(source);

    // splitting up content based on chunking method
    String[] chunks;
    switch (method) {
      case CUSTOM:
        chunks = content.split(regEx);
        break;
      case PARAGRAPH:
        // split any consecutive line break sequence (\n, \r, \r\n) optionally within one unit of whitespace
        // regEx from LangChain4J paragraph splitter
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

    // append chunk to the next available chunk if below a certain number of characters, else append to chunk before.
    if (preMergeMinChunkLen > 0) chunks = filterByAppend(chunks, preMergeMinChunkLen);

    // truncating chunks by number before processing each chunk
    if (preMergeMaxChunkLen > 0) truncateRest(chunks, preMergeMaxChunkLen);

    // either merging by percentage or by number of chunks
    if (chunksToOverlap != null) {
      chunks = mergeAndOverlapChunks(chunks, chunksToMerge, chunksToOverlap);
    } else {
      // merge chunks if we have final chunk size set more than 1
      if (chunksToMerge > 1) chunks = mergeChunks(chunks, chunksToMerge);

      // overlap chunks if we have overlap percentage set
      if (overlapPercentage > 0) chunks = overlapChunks(chunks, overlapPercentage);
    }

    // truncating if we have character limit set
    if (characterLimit > 0) truncateRest(chunks, characterLimit);

    // creating attached children doc for each chunk
    createChildrenDocsWithChunks(doc, chunks);

    return null;
  }

  private String[] mergeAndOverlapChunks(String[] chunks, Integer chunksToMerge, Integer chunksToOverlap) {
    if (isInvalidInput(chunks)) {
      return chunks;
    }

    int chunkLength = chunks.length;
    int stepSize = chunksToMerge - chunksToOverlap;;
    int endIndex = chunkLength - chunksToOverlap;
    int resultSize = (endIndex - 1) / stepSize + 1;
    String[] resultChunks = new String[resultSize];
    StringBuilder sb = new StringBuilder();

    // go through each window and merge them
    for (int i = 0, resultIndex = 0; i < endIndex; i += stepSize, resultIndex++) {
      sb.setLength(0);
      for (int j = i; j < Math.min(i + chunksToMerge, chunkLength); j++) {
        sb.append(chunks[j]).append(" ");
      }
      log.debug("{} {} {}", i, resultIndex, sb.toString());
      resultChunks[resultIndex] = sb.toString().trim();
    }
    return resultChunks;
  }

  // true if no reason to process chunk
  private boolean isInvalidInput(String[] chunks) {
    return chunks.length <= 1;
  }

  private void truncateRest(String[] chunks, int characterLimit) {
    for (int i = 0; i < chunks.length; i++) {
      String s = chunks[i];
      if (s.length() > characterLimit) {
        chunks[i] = s.substring(0, characterLimit).trim();
      }
    }
  }

  public String[] splitBySize(String input, int chunkSize) {
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
      chunks[i] = chunks[i].replaceAll("\\s*(?>\\R)\\s*", " ").trim();
    }
  }

  private String[] filterByAppend(String[] chunks, Integer minimumChunkLength) {
    if (isInvalidInput(chunks)) {
      return chunks;
    }
    int originalChunksLength = chunks.length;

    List<String> finalChunks = new ArrayList<>();
    StringBuilder currentChunk = new StringBuilder();

    for (int i = 0; i < originalChunksLength; i++) {
      currentChunk.append(chunks[i]).append(" ");

      // if current chunk is smaller and is not the last chunk
      if (currentChunk.length() < minimumChunkLength && i + 1 < originalChunksLength) {
        continue;
      }

      // currentChunk not large enough but no next available chunk
      if (currentChunk.length() < minimumChunkLength && i + 1 == originalChunksLength) {
        // append current chunk to last added chunk
        if (!finalChunks.isEmpty()) {
          String chunkToAppend = finalChunks.remove(finalChunks.size() - 1);
          currentChunk = new StringBuilder(chunkToAppend).append(" ").append(currentChunk);
        } else {
          log.warn("all chunks added together will not reach pre merge minimum chunk length. Merging all chunks into one...");
        }
      }

      // add currentChunk to finalChunk and reset currentChunk
      finalChunks.add(currentChunk.toString().trim());
      currentChunk.setLength(0);
    }

    return finalChunks.toArray(new String[0]);
  }

  private String[] mergeChunks(String[] chunks, int chunkSize) {
    if (isInvalidInput(chunks)) {
      return chunks;
    }

    int length = chunks.length;
    int resultSize = (length + chunkSize - 1) / chunkSize;
    String[] result = new String[resultSize];
    StringBuilder sb = new StringBuilder();

    for (int i = 0, chunkIndex = 0; i < resultSize; i++) {
      sb.setLength(0);
      int endIndex = Math.min(chunkIndex + chunkSize, length);
      sb.append(chunks[chunkIndex]);

      for (int j = chunkIndex + 1; j < endIndex; j++) {
        sb.append(' ').append(chunks[j]);
      }

      result[i] = sb.toString().trim();
      chunkIndex = endIndex;
    }

    return result;
  }

  private String[] overlapChunks(String[] chunks, Integer overlapPercentage) {
    if (isInvalidInput(chunks)) {
      return chunks;
    }

    String[] result = new String[chunks.length];
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < chunks.length; i++) {
      // reset sb
      sb.setLength(0);
      sb.append(chunks[i]);

      // calculate the number of characters to overlap
      int overlapChars = chunks[i].length() * overlapPercentage / 100;

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
    if (chunks.length == 0) {
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
