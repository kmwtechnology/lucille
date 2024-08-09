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
import dev.langchain4j.agent.tool.ToolExecutionRequest;
import dev.langchain4j.agent.tool.ToolSpecification;
import dev.langchain4j.data.document.splitter.DocumentByParagraphSplitter;
import dev.langchain4j.data.document.splitter.DocumentByRegexSplitter;
import dev.langchain4j.data.document.splitter.DocumentBySentenceSplitter;
import dev.langchain4j.data.document.splitter.DocumentByWordSplitter;
import dev.langchain4j.data.document.splitter.DocumentSplitters;
import dev.langchain4j.data.message.ChatMessage;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.model.Tokenizer;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import dev.langchain4j.data.document.DocumentSplitter;

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
 *      - chunk itself
 *
 *
 * Things to consider while processing:
 * Token limit (required)
 * chunk overlapping number (optional) -> user may want overlapping chunks to not "lose" information
 * Which model used for token count (optional) -> will use openAI text-embedding-3 model to count tokens as default
 * Chunking Methods (Type Enum, default to fixed size) -> all will use recursive chunking using LangChain4J
 * 1. fixed size chunking
 * 2. paragraph chunking
 * 3. Sentence chunking
 * 4. custom chunking via RegEx -> separator option in config required
 * 5. semantic chunking
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

    // loading the document splitter
    switch (method) {
      case CUSTOM:
        splitter = new DocumentByRegexSplitter(separator, ",", tokenLimit, chunkOverlap, tokenizer);
        break;
      case PARAGRAPH:
        splitter = new DocumentByParagraphSplitter(tokenLimit, chunkOverlap, tokenizer);
        break;
      case SENTENCE:
        splitter = new DocumentBySentenceSplitter(tokenLimit, chunkOverlap, tokenizer);
        break;
      default:
        splitter = new DocumentByWordSplitter(tokenLimit, chunkOverlap, tokenizer);
        break;
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // retrieve content to chunk
    String content = doc.getString(text_field);
    List<String> output = new ArrayList<>();

    // setting up langChain Document and defining splitter
    dev.langchain4j.data.document.Document langDoc = dev.langchain4j.data.document.Document.from(content);

    // split document into Textsegments
    List<TextSegment> textSegments = splitter.split(langDoc);
    // create children doc for each content
    createChildrenDocsWithChunks(doc, textSegments);

    return null;
  }

  private void createChildrenDocsWithChunks(Document doc, List<TextSegment> textSegments) {
    // get the id of the parent
    String parentId = doc.getString(Document.ID_FIELD);

    // create id of child
    for (TextSegment textSegment : textSegments) {
      // TODO: transform childDoc
      Document childDoc = Document.create("");

      // add child to parentDoc
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
