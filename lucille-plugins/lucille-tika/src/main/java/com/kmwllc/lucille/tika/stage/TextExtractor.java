package com.kmwllc.lucille.tika.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.sax.BodyContentHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

/**
 * This stage uses Apache Tika to perform text and metadata extraction
 * <br>
 * Config Parameters -
 * <br>
 * textField (String, Optional) : name of destination field for parsed data to be placed
 * filePathField (String, Optional) : name of field from which file path can be extracted, if filePathField
 * and byteArrayField both not provided, stage will do nothing
 * byteArrayField (String, Optional) : name of field from which byte array data can be extracted
 * tikaConfigPath (String, Optional) : path to tika config, if not provided will default to empty AutoDetectParser
 * metadataPrefix (String, Optional) : prefix to be appended to fields for metadata information extracted after parsing
 * textContentLimit (Integer, Optional) : limits how large the content of the returned text can be
 * parseTimeout (Long, Optional) : timeout for parsing in milliseconds
 * metadataWhitelist (StringList, Optional) : list of metadata names that are to be included in document
 * metadataBlacklist (StringList, Optional) : list of metadata names that are not to be included in document
 *
 * s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.
 * azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.
 * gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 */
public class TextExtractor extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("textField", "filePathField", "byteArrayField", "tikaConfigPath", "metadataPrefix")
      .optionalList("metadataWhitelist", new TypeReference<List<String>>() {
      })
      .optionalList("metadataBlacklist", new TypeReference<List<String>>() {
      })
      .optionalNumber("textContentLimit", "parseTimeout")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .include(FileContentFetcher.SPEC).build();

  private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private String metadataPrefix;
  private Integer textContentLimit;
  private Long parseTimeout;
  private List<String> metadataWhitelist;
  private List<String> metadataBlacklist;
  private Parser parser;
  private ParseContext parseCtx;
  private final FileContentFetcher fileFetcher;
  private ExecutorService executorService;

  public TextExtractor(Config config) throws StageException {
    super(config);

    textField = config.hasPath("textField") ? config.getString("textField") : "text";
    filePathField = config.hasPath("filePathField") ? config.getString("filePathField") : null;
    byteArrayField = config.hasPath("byteArrayField") ? config.getString("byteArrayField") : null;
    metadataPrefix = config.hasPath("metadataPrefix") ? config.getString("metadataPrefix") : "tika";
    tikaConfigPath = config.hasPath("tikaConfigPath") ? config.getString("tikaConfigPath") : null;
    textContentLimit = config.hasPath("textContentLimit") ? config.getInt("textContentLimit") : Integer.MAX_VALUE;
    parseTimeout = config.hasPath("parseTimeout") ? config.getLong("parseTimeout") : null;
    metadataWhitelist = config.hasPath("metadataWhitelist") ? config.getStringList("metadataWhitelist") : null;
    metadataBlacklist = config.hasPath("metadataBlacklist") ? config.getStringList("metadataBlacklist") : null;
    if (metadataWhitelist != null && metadataBlacklist != null) {
      throw new StageException("Provided both a whitelist and blacklist to the TextExtractor stage");
    }
    if (filePathField != null && byteArrayField != null) {
      throw new StageException("Provided both a filePathField and byteArrayField to the TextExtractor stage");
    }
    if (filePathField == null && byteArrayField == null) {
      throw new StageException("Provided neither a filePathField nor byteArrayField to the TextExtractor stage");
    }
    parseCtx = new ParseContext();

    this.fileFetcher = FileContentFetcher.create(config);
  }

  @Override
  public void start() throws StageException {
    // Only try to initialize storage clients for later use if a file path is specified
    if (filePathField != null) {
      try {
        fileFetcher.startup();
      } catch (IOException e) {
        throw new StageException("Error occurred initializing FileContentFetcher.", e);
      }
    }
    if (this.tikaConfigPath == null) {
      parser = new AutoDetectParser();
    } else {
      try {
        File f = new File(this.tikaConfigPath);
        TikaConfig tc = new TikaConfig(f);
        parser = new AutoDetectParser(tc);
      } catch (Exception e) {
        throw new StageException("Error starting TextExtractor stage.", e);
      }
    }
    parseCtx.set(Parser.class, parser);

    if (parseTimeout != null) {
      // each worker is running in a single thread so we only need to run the extraction with a
      // single thread executor rather than using a thread pool.
      executorService = Executors.newSingleThreadExecutor();
    }
  }

  @Override
  public void stop() throws StageException {
    // Shutdown each storage client
    fileFetcher.shutdown();
    if (executorService != null) {
      executorService.shutdownNow();
      try {
        if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
          log.warn("ExecutorService did not terminate in time");
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // if the document has both a byteArray field and a filePathField, only byteArray will be processed.
    if (doc.has(byteArrayField)) {

      byte[] byteArray = doc.getBytes(byteArrayField);

      try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
        parseInputStream(doc, inputStream);
      } catch (IOException e) {
        log.warn("Error closing inputStream: ", e);
        return null;
      }

    } else if (doc.has(filePathField)) {
      // get fileObject from path
      String filePath = doc.getString(filePathField);

      try (InputStream contentStream = fileFetcher.getInputStream(filePath)) {
        parseInputStream(doc, contentStream);
      } catch (Exception e) {
        log.warn("Error with InputStream for file path.", e);
      }
    }
    return null;
  }

  /**
   * Cleans the name of metadata field names to be in line with general standards for documents
   */
  private static String cleanFieldName(String name) {
    String cleanName = name.trim().toLowerCase();
    cleanName = cleanName.replaceAll(" ", "_");
    cleanName = cleanName.replaceAll("-", "_");
    cleanName = cleanName.replaceAll(":", "_");
    return cleanName;
  }

  /**
   * Parses given input stream, close it, and adds the text data and metadata to given document
   */
  public void parseInputStream(Document doc, InputStream inputStream) {
    Metadata metadata = new Metadata();
    ContentHandler bch = new BodyContentHandler(textContentLimit);
    if (parseTimeout == null) {
      parse(doc, inputStream, metadata, bch);
    } else {
      Future<?> future = executorService.submit(() -> {
        parse(doc, inputStream, metadata, bch);
      });

      try {
        future.get(parseTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        future.cancel(true);
        log.warn("Tika parsing timed out after {} ms", parseTimeout);
      } catch (Exception e) {
        log.warn("Error during async Tika parsing: {}", e.getMessage());
      }
    }

    doc.setOrAdd(textField, bch.toString());
    String newMetadataPrefix = metadataPrefix == "" ? "" : metadataPrefix + "_";
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      if ((metadataBlacklist != null && !metadataBlacklist.contains(cleanName))
          || (metadataWhitelist != null && metadataWhitelist.contains(cleanName))
          || (metadataWhitelist == null) && (metadataBlacklist == null)) {
        for (String value : metadata.getValues(name)) {
          doc.setOrAdd(newMetadataPrefix + cleanName, value);
        }
      }
    }
  }

  private void parse(Document doc, InputStream inputStream, Metadata metadata, ContentHandler bch) {
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e.getMessage());
    }
  }
}