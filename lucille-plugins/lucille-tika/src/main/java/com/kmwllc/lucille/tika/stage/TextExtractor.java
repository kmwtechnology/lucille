package com.kmwllc.lucille.tika.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FieldFilter;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
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
 * whitelist (StringList, Optional) : list of metadata names that are to be included in document
 * blacklist (StringList, Optional) : list of metadata names that are not to be included in document
 * fieldNamesField (String, Optional) : if set, each extracted metadata field's prefixed name is added as a separate value to this
 * multi-valued field.
 * s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.
 * azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.
 * gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 * metadataFields (Map, Optional) : Maps Tika metadata keys to the names of document fields whose values should be
 * copied into the {@link Metadata} handed to Tika <i>before</i> parsing. This is the input to Tika, and is distinct
 * from the metadata Tika produces <i>after</i> parsing (which is written back to the document under metadataPrefix).
 * <br>
 * The primary use case is supplying content-type detection hints. Because this stage parses from a plain
 * {@link InputStream}, Tika's container-aware detectors are skipped and detection falls back to "Mime Magic" on the
 * leading bytes alone. Providing hints lets Tika refine or override that guess. The two most useful keys are:
 * <ul>
 *   <li>{@link TikaCoreProperties#RESOURCE_NAME_KEY} ("resourceName") - the file name. Lets Tika use the extension,
 *   e.g. to distinguish a CSV from generic text, or to pick the right parser when magic bytes are ambiguous.</li>
 *   <li>{@link Metadata#CONTENT_TYPE} ("Content-Type") - an advertised content type, such as one returned by a web
 *   server or a source repository.</li>
 * </ul>
 * Each map value is the name of the document field to read the hint from; document fields that are absent are skipped.
 * See <a href="https://tika.apache.org/3.2.3/detection.html">Tika content type detection</a> for details. Example: a
 * document with "filename" and "content_type" fields would be configured as
 * {
 *   "metadataFields": {
 *     "resourceName": "filename",
 *     "Content-Type": "content_type"
 *   }
 * }
 */
public class TextExtractor extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("textField", "filePathField", "byteArrayField", "tikaConfigPath", "metadataPrefix", "fieldNamesField")
      .optionalList("whitelist", new TypeReference<List<String>>() {})
      .optionalList("blacklist", new TypeReference<List<String>>() {})
      .optionalNumber("textContentLimit", "parseTimeout")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .optionalParent("metadataFields", new TypeReference<Map<String, Object>>() {})
      .include(FileContentFetcher.SPEC).build();

  private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private String metadataPrefix;
  private String fieldNamesField;
  private Integer textContentLimit;
  private Long parseTimeout;
  private Parser parser;
  private ParseContext parseCtx;
  private final FileContentFetcher fileFetcher;
  private ExecutorService executorService;
  private final FieldFilter fieldFilter;
  private final Map<String, Object> metadataFields;

  public TextExtractor(Config config) throws StageException {
    super(config);

    textField = config.hasPath("textField") ? config.getString("textField") : "text";
    filePathField = config.hasPath("filePathField") ? config.getString("filePathField") : null;
    byteArrayField = config.hasPath("byteArrayField") ? config.getString("byteArrayField") : null;
    metadataPrefix = config.hasPath("metadataPrefix") ? config.getString("metadataPrefix") : "tika";
    tikaConfigPath = config.hasPath("tikaConfigPath") ? config.getString("tikaConfigPath") : null;
    textContentLimit = config.hasPath("textContentLimit") ? config.getInt("textContentLimit") : Integer.MAX_VALUE;
    parseTimeout = config.hasPath("parseTimeout") ? config.getLong("parseTimeout") : null;
    fieldNamesField = config.hasPath("fieldNamesField") ? config.getString("fieldNamesField") : null;
    metadataFields = config.hasPath("metadataFields") ? config.getConfig("metadataFields").root().unwrapped() : null;

    this.fieldFilter = new FieldFilter(config);

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

    Metadata metadata = createMetadataInput(doc);

    if (doc.has(byteArrayField)) {

      byte[] byteArray = doc.getBytes(byteArrayField);

      try (InputStream inputStream = new ByteArrayInputStream(byteArray)) {
        parseInputStream(metadata, doc, inputStream);
      } catch (IOException e) {
        log.warn("Error closing inputStream: ", e);
        return null;
      }

    } else if (doc.has(filePathField)) {
      // get fileObject from path
      String filePath = doc.getString(filePathField);

      try (InputStream contentStream = fileFetcher.getInputStream(filePath)) {
        parseInputStream(metadata, doc, contentStream);
      } catch (Exception e) {
        log.warn("Error with InputStream for file path.", e);
      }
    }
    return null;
  }

  /**
   * Builds the input {@link Metadata} handed to Tika before parsing, copying values from the given document into the
   * metadata according to the configured {@code metadataFields} mapping.
   *
   * <p>Each entry maps a Tika metadata key to the name of a document field to read from. Fields the document does not
   * contain are skipped, and multi-valued fields contribute all of their values. These values act as detection hints
   * (see the class-level documentation), not as extracted output.
   *
   * @param doc the document to read the configured hint fields from.
   * @return a {@code Metadata} populated with the configured hints (empty when {@code metadataFields} is unset).
   */
  public Metadata createMetadataInput(Document doc) {
    Metadata metadata = new Metadata();
    if (metadataFields != null) {
      for (Map.Entry<String, Object> entry : metadataFields.entrySet()) {
        String metadataKey = entry.getKey();
        String docFieldName = entry.getValue().toString();
        if (doc.has(docFieldName)) {
          for (String value : doc.getStringList(docFieldName)) {
            metadata.add(metadataKey, value);
          }
        }
      }
    }
    return metadata;
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
   * Parses the given input stream, extracts text content and metadata, and adds them to the provided document.
   *
   * @param metadata A {@code Metadata} object used to hold metadata extracted from the input stream.
   * @param doc A {@code Document} object where the extracted text data and metadata will be stored.
   * @param inputStream The {@code InputStream} containing the content to be parsed.
   */
  public void parseInputStream(Metadata metadata, Document doc, InputStream inputStream) {
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
    String newMetadataPrefix = metadataPrefix.isEmpty() ? "" : metadataPrefix + "_";
    for (String name : metadata.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name);
      if (fieldFilter.shouldInclude(cleanName)) {
        String prefixedName = newMetadataPrefix + cleanName;
        for (String value : metadata.getValues(name)) {
          doc.setOrAdd(prefixedName, value);
        }
        if (fieldNamesField != null) {
          doc.setOrAdd(fieldNamesField, prefixedName);
        }
      }
    }
  }

  /**
   * Parses given input stream, close it, and adds the text data and metadata to given document
   */
  public void parseInputStream(Document doc, InputStream inputStream) {
    Metadata metadata = new Metadata();
    parseInputStream(metadata, doc, inputStream);
  }

  private void parse(Document doc, InputStream inputStream, Metadata metadata, ContentHandler bch) {
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      log.warn("Tika Exception: {}", e.getMessage());
    }
  }
}