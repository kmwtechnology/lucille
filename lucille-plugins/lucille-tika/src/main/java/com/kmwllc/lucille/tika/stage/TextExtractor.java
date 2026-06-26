package com.kmwllc.lucille.tika.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConfigUtils;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaException;
import org.apache.tika.fork.ForkParser;
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
 * parseTimeout (Long, Optional) : timeout for parsing in milliseconds.
 * whitelist (List&lt;String&gt;, Optional) : list of metadata names that are to be included in document
 * blacklist (List&lt;String&gt;, Optional) : list of metadata names that are not to be included in document
 * fieldNamesField (String, Optional) : if set, each extracted metadata field's prefixed name is added as a separate value to this
 * multi-valued field.
 * fork.enabled (Boolean, Optional) : Whether parsing should be run in a child JVM via the <code>ForkParser</code>.
 * This adds overhead to each document but isolates OOM crashes from the parent. Defaults to false.
 * fork.poolSize (Integer, Optional) : number of child JVM processes kept alive in the pool. Defaults to 5.
 * fork.jvmArgs (List&lt;String&gt;, Optional) : JVM command and arguments for each child process. Defaults to ["java", "-Djava.awt.headless=true"].
 * "java" will be prepended to the jvmArgs, whenever you specify them.
 * fork.serverPulseMillis (Long, Optional) : Heartbeat interval (in ms) between parent and child processes. Defaults to 1000.
 * s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.
 * azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.
 * gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 */
public class TextExtractor extends Stage {

  public static final Spec FORK_SPEC = SpecBuilder.parent("fork")
      .optionalBoolean("enabled")
      .optionalList("jvmArgs", new TypeReference<List<String>>() {})
      .optionalNumber("poolSize", "serverPulseMillis")
      .build();

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("textField", "filePathField", "byteArrayField", "tikaConfigPath", "metadataPrefix", "fieldNamesField")
      .optionalList("whitelist", new TypeReference<List<String>>() {})
      .optionalList("blacklist", new TypeReference<List<String>>() {})
      .optionalNumber("textContentLimit", "parseTimeout")
      .optionalParent(FORK_SPEC, FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .include(FileContentFetcher.SPEC).build();

  private static final List<String> DEFAULT_FORK_JVM_ARGS =
      Arrays.asList("java", "-Djava.awt.headless=true");

  private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);
  private String textField;
  private String filePathField;
  private String tikaConfigPath;
  private String byteArrayField;
  private String metadataPrefix;
  private String fieldNamesField;
  private Integer textContentLimit;
  private Long parseTimeout;
  private boolean forkEnabled;
  private int forkPoolSize;
  private List<String> forkJvmArgs;
  private long forkServerPulseMillis;
  private Parser parser;
  private ForkParser forkParser;
  private ParseContext parseCtx;
  private final FileContentFetcher fileFetcher;
  private ExecutorService executorService;
  private final FieldFilter fieldFilter;

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

    forkEnabled = ConfigUtils.getOrDefault(config, "fork.enabled", false);
    forkPoolSize = ConfigUtils.getOrDefault(config, "fork.poolSize", 5);

    if (config.hasPath("fork.jvmArgs")) {
      forkJvmArgs = config.getStringList("fork.jvmArgs");
      // always prepend java so users don't need to specify it
      forkJvmArgs.add(0, "java");
    } else {
      forkJvmArgs = DEFAULT_FORK_JVM_ARGS;
    }

    forkServerPulseMillis = ConfigUtils.getOrDefault(config, "fork.serverPulseMillis", 1000L);

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

    AutoDetectParser autoParser;
    if (this.tikaConfigPath == null) {
      autoParser = new AutoDetectParser();
    } else {
      try {
        File f = new File(this.tikaConfigPath);
        TikaConfig tc = new TikaConfig(f);
        autoParser = new AutoDetectParser(tc);
      } catch (Exception e) {
        throw new StageException("Error starting TextExtractor stage.", e);
      }
    }

    if (forkEnabled) {
      forkParser = new ForkParser(TextExtractor.class.getClassLoader(), autoParser);
      forkParser.setPoolSize(forkPoolSize);
      forkParser.setJavaCommand(forkJvmArgs);
      forkParser.setServerPulseMillis(forkServerPulseMillis);
      if (parseTimeout != null) {
        forkParser.setServerParseTimeoutMillis(parseTimeout);
      }
      parser = forkParser;
    } else {
      parser = autoParser;
      parseCtx.set(Parser.class, parser);
      if (parseTimeout != null) {
        // each worker is running in a single thread so we only need to run the extraction with a
        // single thread executor rather than using a thread pool.
        executorService = Executors.newSingleThreadExecutor();
      }
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
    if (forkParser != null) {
      forkParser.close();
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
      }

    } else if (doc.has(filePathField)) {
      // get fileObject from path
      String filePath = doc.getString(filePathField);

      try (InputStream contentStream = fileFetcher.getInputStream(filePath)) {
        parseInputStream(doc, contentStream);
      } catch (IOException e) {
        log.warn("Error processing file {}", filePath, e);
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
  public void parseInputStream(Document doc, InputStream inputStream) throws StageException {
    Metadata metadata = new Metadata();
    ContentHandler bch = new BodyContentHandler(textContentLimit);
    if (forkEnabled || parseTimeout == null) {
      // fork path: StageException propagates on child failure; ForkParser's serverParseTimeoutMillis handles hangs.
      // non-fork path with no timeout: parse inline.
      parse(doc, inputStream, metadata, bch);
    } else {
      Future<?> future = executorService.submit(() -> {
        try {
          parse(doc, inputStream, metadata, bch);
        } catch (StageException e) {
          log.warn("Tika Exception: {}", e.getMessage());
        }
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

  private void parse(Document doc, InputStream inputStream, Metadata metadata, ContentHandler bch) throws StageException {
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      if (forkEnabled) {
        throw new StageException("Forked Tika process failed for document: " + doc.getId(), e);
      }
      log.warn("Tika Exception: {}", e.getMessage());
    }
  }
}
