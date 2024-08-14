package com.kmwllc.lucille.stage;

import static dev.langchain4j.store.embedding.filter.MetadataFilterBuilder.metadataKey;

import com.kmwllc.lucille.core.ChunkingMethod;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.knuddels.jtokkit.Encodings;
import com.knuddels.jtokkit.api.Encoding;
import com.knuddels.jtokkit.api.ModelType;
import com.typesafe.config.Config;
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.document.splitter.DocumentByParagraphSplitter;
import dev.langchain4j.data.document.splitter.DocumentByRegexSplitter;
import dev.langchain4j.data.document.splitter.DocumentBySentenceSplitter;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import dev.langchain4j.store.embedding.inmemory.InMemoryEmbeddingStore;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.Tokenizer;
import dev.langchain4j.model.embedding.EmbeddingModel;
import dev.langchain4j.model.embedding.onnx.allminilml6v2q.AllMiniLmL6V2QuantizedEmbeddingModel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import dev.langchain4j.data.document.DocumentSplitter;
import org.w3c.dom.Text;

/**
 * Process of Chunking Stage:
 * - retrieve text information from a Lucille document field
 * - process text to chunks
 * - create child document for each chunk
 *  - child document fields:
 *      - id
 *      - id of parent Document
 *      - offset from start of document (tokens)
 *      - length (tokens)
 *      - chunk number
 *      - TODO: total chunk number maybe?
 *      - chunk itself
 *
 * Things to consider while processing:
 * Token limit (required)
 * chunk overlapping number (optional) -> user may want to overlap chunks to not "lose" information
 * Which model used for token count (optional) -> will use openAI text-embedding-3 model to count tokens as default
 * Chunking Methods (Type Enum, default to fixed size) -> all will use recursive chunking using LangChain4J
 * 1. fixed size chunking -> same as sentence or want Naively chunking
 * 2. paragraph chunking
 * 3. Sentence chunking
 * 4. custom chunking via RegEx -> separator option in config required
 *
 * 5. semantic chunking -> very intensive, another stage?
 *  - threshold types: percentile, standard_deviation, interquartile
 *  -
 */

public class Chunking extends Stage {

  private final Integer tokenLimit;
  private final Integer chunkOverlap;
  private final ChunkingMethod method;
  private final String separator;
  private final String outputName;
  private final String text_field;
  private Encoding enc;
  private DocumentSplitter splitter;
  private Tokenizer tokenizer;
  private int bufferSize;
  private double threshold;
  private EmbeddingModel embeddingModel;
  private InMemoryEmbeddingStore<TextSegment> embeddingStore;
  private static final Logger log = LogManager.getLogger(Chunking.class);

  public Chunking(Config config) throws StageException {
    super(config, new StageSpec()
        .withOptionalProperties("chunking_method", "chunk_overlap", "output_name", "separator")
        .withRequiredProperties("token_limit", "text_field"));
    tokenLimit = config.getInt("token_limit");
    chunkOverlap = config.hasPath("chunk_overlap") ? config.getInt("chunk_overlap") : 0;
    method = ChunkingMethod.fromConfig(config);
    separator = config.hasPath("separator") ? config.getString("separator") : "";
    text_field = config.getString("text_field");
    outputName = config.hasPath("output_name") ? config.getString("output_name") : "chunk";
    bufferSize = config.hasPath("buffer_size") ? config.getInt("buffer_size") : 0;
    threshold = config.hasPath("threshold") ? config.getDouble("threshold") : 0.7;
    if (tokenLimit <= 0) {
      throw new StageException("Provide a positive token_limit");
    }
    if (text_field == null) {
      throw new StageException("text_field configuration where Chunk Stage would take as input is missing");
    }
    if (method == ChunkingMethod.CUSTOM && separator.isEmpty()) {
      throw new StageException("Provide a non empty regEx for separator configuration");
    }
  }

  /**
   *
   * @throws StageException if the field mapping is empty.
   */
  @Override
  public void start() throws StageException {
    // load the encoding type
    String modelTypeStr = config.hasPath("model_type") ? config.getString("model_type") : "text-embedding-3-large";
    ModelType modelType = ModelType.fromName(modelTypeStr).orElse(ModelType.TEXT_EMBEDDING_3_LARGE);
    enc = Encodings.newDefaultEncodingRegistry().getEncodingForModel(modelType);

    // load respective tokenizer
    tokenizer = new OurTokenizer();

    if (method == ChunkingMethod.SEMANTIC) {
      // creates an in-process local embedding model and embedding store //TODO: what kind of embedding model should I use?
      embeddingModel = new AllMiniLmL6V2QuantizedEmbeddingModel();
      embeddingStore = new InMemoryEmbeddingStore<>();
    }

    // loading the document splitter
    switch (method) {
      case CUSTOM:
        splitter = new DocumentByRegexSplitter(separator, ",", tokenLimit, chunkOverlap, tokenizer);
        break;
      case PARAGRAPH:
        // splits by paragraph, if still too long, splits by sentence, and if still too long, split by words
        splitter = new DocumentByParagraphSplitter(tokenLimit, chunkOverlap, tokenizer);
        break;
      default: // FIXED, SENTENCE, SEMANTIC all use Sentences to split
        splitter = new DocumentBySentenceSplitter(tokenLimit, chunkOverlap, tokenizer);
        break;
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // retrieve content to chunk
    String content = doc.getString(text_field);

    // setting up langChain Document and defining splitter
    dev.langchain4j.data.document.Document langDoc = dev.langchain4j.data.document.Document.from(content);

    // split document into TextSegments
    List<TextSegment> textSegments = splitter.split(langDoc);

    if (textSegments == null || textSegments.isEmpty()) {
      log.warn("No text segments found");
      return null;
    }

    // create metadata token_length for all textSegments
    for (TextSegment textSegment : textSegments) {
      textSegment.metadata().put("token_length", enc.countTokens(textSegment.text()));
      //for testing: System.out.println(textSegment.metadata().getInteger("token_length"));
    }

    if (method == ChunkingMethod.SEMANTIC) {
      textSegments = semanticChunk(textSegments);
    }

    // create children doc for each content
    createChildrenDocsWithChunks(doc, textSegments);

    return null;
  }

  private List<TextSegment> semanticChunk(List<TextSegment> textSegments) {
    // create groups based on buffer size, but keeping within tokenLimit
    List<TextSegment> groups = createGroups(textSegments);

    // create embeddings and merge similar groups keeping within tokenLimit
    List<TextSegment> mergedGroups = mergeSimilarGroups(groups, embeddingModel);

    return mergedGroups;
  }

  private List<TextSegment> createGroups(List<TextSegment> textSegments) {
    // if textSegments is not big enough to create groups, return
    if (textSegments.size() <= 1) return textSegments;

    // creating groups
    List<TextSegment> groups = new ArrayList<>();
    // this for loop goes through each sentence and tries to add buffer to it (its neighbours)
    for (int i = 0; i < textSegments.size(); i++) {
      StringBuilder combinedSentences = new StringBuilder();
      int currentTokenCount = textSegments.get(i).metadata().getInteger("token_count");

      // adding buffer (checking left then right)
      int left = i-1;
      int right = i+1;
      int currentBufferCount = 0;
      combinedSentences.append(textSegments.get(i).text());
      // will exit if left and right exceed buffer size or if token limit has been reached
      while (isValid(bufferSize, left, right, i)) {
        // ignore if left is less than 0
        if (left >= 0) {
          TextSegment textSegment = textSegments.get(left);
          currentBufferCount += textSegment.metadata().getInteger("token_count");
          if (currentTokenCount + currentBufferCount > tokenLimit) {
            break;
          }
          // insert to the front
          combinedSentences.insert(0, textSegment.text()).append(" ");
        }
        // ignore if right is more than list size
        if (right < textSegments.size()) {
          TextSegment textSegment = textSegments.get(right);
          currentBufferCount += textSegment.metadata().getInteger("token_count");
          if (currentTokenCount + currentBufferCount > tokenLimit) {
            break;
          }
          // insert to the back
          combinedSentences.append(textSegment.text());
        }
        // increment the pointers
        left--;
        right++;
      }

      // add the combined sentences as Text Segments
      TextSegment sentences = TextSegment.from(combinedSentences.toString());
      sentences.metadata().put("token_count", currentTokenCount + currentBufferCount);
      groups.add(sentences);
    }

    return groups;
  }

  private boolean isValid(int bufferSize, int left, int right, int middle) {
    return middle - left <= bufferSize && right - middle <= bufferSize;
  }


  private List<TextSegment> mergeSimilarGroups(List<TextSegment> groups, EmbeddingModel embeddingModel) {
    // if list is not greater than 1 or then no point merging //TODO two? if merge it becomes one anyway
    if (groups.size() <= 1) return groups;

    List<TextSegment> mergedGroups = new ArrayList<>();

    // add all to embedding store
    for (int i = 0; i < groups.size(); i++) {
      // assign id using index to each group so that we can delete from embedding store in future
      TextSegment group = groups.get(i);
      String groupId = String.valueOf(i);
      group.metadata().put("id", groupId);

      Embedding embedding = embeddingModel.embed(group.text()).content();
      embeddingStore.add(groupId, embedding);
    }

    // try to merge groups if similar enough and within token limit
    for (TextSegment group : groups) {
      int currTokenCount = group.metadata().getInteger("token_count");
      int remainingTokenCount = tokenLimit - currTokenCount;
      Embedding embedding = embeddingModel.embed(group.text()).content();
      EmbeddingSearchRequest request = new EmbeddingSearchRequest(embedding, groups.size(), threshold,
          metadataKey("token_count").isLessThanOrEqualTo(remainingTokenCount));

      List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();

      if (matches.isEmpty()) {
        mergedGroups.add(group);
        // remove TextSegment from store
        String id = group.metadata().getString("id");
        embeddingStore.remove(id);
      } else {
        StringBuilder newGroup = new StringBuilder(group.text());
        List<String> idsMerged = new ArrayList<>();
        for (EmbeddingMatch<TextSegment> match : matches) {
          TextSegment matchSegment = match.embedded();
          if (currTokenCount + matchSegment.metadata().getInteger("token_count") > tokenLimit) {
            break;
          }
          String matchId = matchSegment.metadata().getString("id");
          newGroup.append(matchSegment.text());

          currTokenCount += matchSegment.metadata().getInteger("token_count");
          idsMerged.add(matchId);
        }

        // delete all merged ids from store
        embeddingStore.removeAll(idsMerged);

        // add new group to mergedGroups
        TextSegment newSegment = TextSegment.from(newGroup.toString());
        newSegment.metadata().put("token_count", currTokenCount);
        mergedGroups.add(newSegment);
      }
    }

    // add remaining vectors in store add into merged groups
    EmbeddingSearchRequest request = new EmbeddingSearchRequest(embeddingModel.embed(groups.get(0).text()).content(), groups.size(),
        0.0, metadataKey("token_count").isLessThanOrEqualTo(tokenLimit));
    List<EmbeddingMatch<TextSegment>> matches = embeddingStore.search(request).matches();
    for (EmbeddingMatch<TextSegment> match : matches) {
      TextSegment matchSegment = match.embedded();
      mergedGroups.add(matchSegment);
    }

    embeddingStore.removeAll();
    return mergedGroups;
  }

  private void createChildrenDocsWithChunks(Document doc, List<TextSegment> textSegments) {
    if (textSegments == null || textSegments.isEmpty()) {
      return;
    }
    // get the id of the parent
    String parentId = doc.getString(Document.ID_FIELD);
    int offset = 0;

    /*
     *      - id
     *      - id of parent Document
     *      - offset from start of document (tokens)
     *      - length (tokens)
     *      - chunk number
     *      - chunk itself
     */

    for (int i = 0; i < textSegments.size(); i++) {
      String id = parentId + "-" + (i + 1); // TODO: how should children id be generated?
      Document childDoc = Document.create(id);

      childDoc.setField("parent_id", parentId);

      childDoc.setField("chunk_number", i + 1);

      childDoc.setField("offset", offset);

      int tokenLength = textSegments.get(i).metadata().getInteger("token_length");
      childDoc.setField("length", tokenLength);
      offset += tokenLength;

      String chunk = textSegments.get(i).text();
      childDoc.setField(outputName, chunk);

      doc.addChild(childDoc);
    }
  }


  private class OurTokenizer implements Tokenizer {

    @Override
    public int estimateTokenCountInText(String s) {
      return enc.countTokens(s);
    }

    @Override
    public int estimateTokenCountInMessage(ChatMessage chatMessage) {
      return enc.countTokens(chatMessage.toString());
    }

    @Override
    public int estimateTokenCountInMessages(Iterable<ChatMessage> iterable) {
      int count = 0;
      for (ChatMessage chatMessage : iterable) {
        count += estimateTokenCountInMessage(chatMessage);
      }
      return count;
    }

    @Override
    public int estimateTokenCountInToolSpecifications(Iterable<ToolSpecification> iterable) {
      int count = 0;
      for (ToolSpecification toolSpecification : iterable) {
        count += enc.countTokens(toolSpecification.toString());
      }
      return count;
    }

    @Override
    public int estimateTokenCountInToolExecutionRequests(Iterable<ToolExecutionRequest> iterable) {
      int count = 0;
      for (ToolExecutionRequest toolExecutionRequest : iterable) {
        count += enc.countTokens(toolExecutionRequest.toString());
      }
      return count;
    }
  }
}
