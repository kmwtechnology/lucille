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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.exception.TikaException;
import org.apache.tika.exception.WriteLimitReachedException;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.fork.ForkParser;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaCoreProperties;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRParser;
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
 * tikaConfigPath (String, Optional) : path to tika config, used as is. If not provided will default to empty
 * AutoDetectParser, without OCR unless enableOcr is true.
 * enableOcr (Boolean, Optional) : whether to OCR images and image-only PDF pages with Tika's TesseractOCRParser, which
 * runs the external "tesseract" binary. Defaults to false: the parser is built without TesseractOCRParser, so
 * images yield metadata only, and PDF pages are not rendered for OCR because PDFParser's default AUTO OCR strategy
 * finds no parser for them. Before this option existed, OCR ran implicitly whenever a tesseract binary was on the PATH;
 * set enableOcr to true to keep that behaviour. Cannot be combined with tikaConfigPath (the stage fails to construct):
 * when using a Tika config, control OCR in that config instead, e.g. with a
 * &lt;parser-exclude class="org.apache.tika.parser.ocr.TesseractOCRParser"/&gt; under the DefaultParser.
 * A global Tika config (the "tika.config" system property or the TIKA_CONFIG environment variable) is treated the
 * same way when tikaConfigPath is not set: it is used as is, and enableOcr cannot be combined with it.
 * A provided or global config is applied to embedded documents as well as the top-level document.
 * metadataPrefix (String, Optional) : prefix to be appended to fields for metadata information extracted after parsing
 * textContentLimit (Integer, Optional) : limits how large the content of the returned text can be
 * parseTimeout (Long, Optional) : timeout for parsing in milliseconds. On timeout, the text extracted so far and the
 * metadataFields input hints are added to the document, but no extracted metadata.
 * skipEmbeddedContentTypePrefixes (List&lt;String&gt;, Optional) : content-type prefixes for which embedded documents are
 * skipped (not parsed), e.g. ["image/"] to skip embedded images. Defaults to empty (all embedded documents parsed).
 * Embedded parts with no content type are always parsed.
 * whitelist (List&lt;String&gt;, Optional) : list of metadata names that are to be included in document
 * blacklist (List&lt;String&gt;, Optional) : list of metadata names that are not to be included in document
 * fieldNamesField (String, Optional) : if set, each extracted metadata field's prefixed name is added as a separate value to this
 * multi-valued field.
 * replaceDotsInMetadataNames (Boolean, Optional) : if true, "." in metadata names is replaced with "_", as spaces, "-" and ":"
 * always are. Defaults to false. Recommended when indexing into OpenSearch or Elasticsearch, which treat a dotted field
 * name as an object path: Tika emits keys such as "dc.date" and "dc.date.created", whose dotted field names cannot both be
 * mapped, so the document is rejected. Applies to metadata names only, not to metadataPrefix. When enabled, whitelist and
 * blacklist entries must use the replaced form (e.g. "dc_date"; dotted entries never match and are logged as a warning), and a dotted key that collapses onto an existing name (e.g. "dc.date" and "dc_date") has its values added
 * to the same multi-valued field.
 * fork.enabled (Boolean, Optional) : Whether parsing should be run in a child JVM via the <code>ForkParser</code>.
 * This adds overhead to each document but isolates OOM crashes from the parent. Defaults to false.
 * fork.poolSize (Integer, Optional) : number of child JVM processes kept alive in the pool. Defaults to 5.
 * fork.jvmArgs (List&lt;String&gt;, Optional) : JVM command and arguments for each child process. Defaults to ["java", "-Djava.awt.headless=true"].
 * "java" will be prepended to the jvmArgs, whenever you specify them.
 * fork.serverPulseMillis (Long, Optional) : Heartbeat interval (in ms) between parent and child processes. Defaults to 1000.
 * s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.
 * azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.
 * gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 * storageClients (Map, Optional) : If your files are held in storage with a custom StorageClient. See FileConnector for the appropriate arguments to provide.
 * metadataFields (Map, Optional) : Maps Tika metadata keys to the names of document fields whose values should be
 * copied into the {@link Metadata} handed to Tika <i>before</i> parsing. This is the input to Tika, and is distinct
 * from the metadata Tika produces <i>after</i> parsing (Both input and output metadata are written back to the document under
 * metadataPrefix unless they are filtered out by the whitelist and/or blacklist configuration).
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

  public static final Spec FORK_SPEC = SpecBuilder.parent("fork")
      .optionalBoolean("enabled")
      .optionalList("jvmArgs", new TypeReference<List<String>>() {})
      .optionalNumber("poolSize", "serverPulseMillis")
      .build();

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("textField", "filePathField", "byteArrayField", "tikaConfigPath", "metadataPrefix", "fieldNamesField")
      .optionalBoolean("replaceDotsInMetadataNames")
      .optionalList("whitelist", new TypeReference<List<String>>() {})
      .optionalList("blacklist", new TypeReference<List<String>>() {})
      .optionalNumber("textContentLimit", "parseTimeout")
      .optionalList("skipEmbeddedContentTypePrefixes", new TypeReference<List<String>>() {})
      .optionalBoolean("enableOcr")
      .optionalParent(FORK_SPEC, FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .optionalParent(FileConnector.STORAGE_CLIENTS, FileConnector.STORAGE_CLIENTS_TYPE)
      .optionalParent("metadataFields", new TypeReference<Map<String, Object>>() {})
      .include(FileContentFetcher.SPEC).build();

  // Default Tika config minus the Tesseract parser. With no parser able to OCR, PDFParser also skips rendering pages for OCR.
  private static final String NO_OCR_TIKA_CONFIG = "<properties><parsers>"
      + "<parser class=\"org.apache.tika.parser.DefaultParser\">"
      + "<parser-exclude class=\"" + TesseractOCRParser.class.getName() + "\"/>"
      + "</parser></parsers></properties>";

  private static final List<String> DEFAULT_FORK_JVM_ARGS =
      Arrays.asList("java", "-Djava.awt.headless=true");

  private static final Logger log = LoggerFactory.getLogger(TextExtractor.class);

  // numbers parse threads so they can be told apart in thread dumps
  private static final AtomicInteger PARSE_THREAD_COUNT = new AtomicInteger();

  // Building TikaConfigs from files is expensive, so we cache these configs across the JVM.
  // This is particularly helpful in multi-threaded runs, where each thread has its own instance of
  // TextExtractor but is using the same TikaConfig.
  // The cache is bounded by MAX_CACHED_CONFIGS; once it is reached, the oldest entry is evicted (FIFO).
  // Keys are canonical paths; each value holds the file's last-modified time alongside the config built from it,
  // so an edited file is rebuilt and replaces its entry.
  private static final int MAX_CACHED_CONFIGS = 1000;
  private static final Map<String, Pair<Long, TikaConfig>> TIKA_CONFIG_CACHE = Collections.synchronizedMap(
      new LinkedHashMap<>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Pair<Long, TikaConfig>> eldest) {
          return size() > MAX_CACHED_CONFIGS;
        }
      });

  private final String textField;
  protected String filePathField;
  private final String tikaConfigPath;
  protected String byteArrayField;
  private final String metadataPrefix;
  private final String fieldNamesField;
  private final boolean replaceDotsInMetadataNames;
  private final Integer textContentLimit;
  private final Long parseTimeout;
  private final boolean enableOcr;
  // where the global Tika config honoured by Tika's default constructors comes from (e.g. "tika.config system
  // property"), or null if there is none; a description for messages, not the config itself
  private final String globalTikaConfigSource;
  private final boolean forkEnabled;
  private final int forkPoolSize;
  private List<String> forkJvmArgs;
  private final long forkServerPulseMillis;
  private Parser parser;
  private ForkParser forkParser;
  private TikaConfig tikaConfig;
  private DocumentSelector documentSelector;
  protected final FileContentFetcher fileFetcher;
  private ExecutorService executorService;
  private final FieldFilter fieldFilter;
  private final Map<String, Object> metadataFields;
  private final List<String> skipEmbeddedContentTypePrefixes;

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
    replaceDotsInMetadataNames = config.hasPath("replaceDotsInMetadataNames") && config.getBoolean("replaceDotsInMetadataNames");
    metadataFields = config.hasPath("metadataFields") ? config.getConfig("metadataFields").root().unwrapped() : null;
    skipEmbeddedContentTypePrefixes = config.hasPath("skipEmbeddedContentTypePrefixes")
        ? config.getStringList("skipEmbeddedContentTypePrefixes")
        : List.of();
    enableOcr = ConfigUtils.getOrDefault(config, "enableOcr", false);
    globalTikaConfigSource = tikaConfigPath == null ? detectGlobalTikaConfigSource() : null;

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
    if (replaceDotsInMetadataNames) {
      for (String listName : List.of("whitelist", "blacklist")) {
        if (config.hasPath(listName) && config.getStringList(listName).stream().anyMatch(n -> n.contains("."))) {
          log.warn("replaceDotsInMetadataNames is enabled but {} contains a dotted name, which will never match; use \"_\" instead of \".\"", listName);
        }
      }
    }

    if (filePathField != null && byteArrayField != null) {
      throw new StageException("Provided both a filePathField and byteArrayField to the TextExtractor stage");
    }
    if (filePathField == null && byteArrayField == null) {
      throw new StageException("Provided neither a filePathField nor byteArrayField to the TextExtractor stage");
    }
    // DocumentSelector can't cross the fork boundary (not Serializable), so skipping can't be applied there.
    if (forkEnabled && !skipEmbeddedContentTypePrefixes.isEmpty()) {
      throw new StageException("skipEmbeddedContentTypePrefixes is not supported when fork.enabled is true.");
    }
    if (tikaConfigPath != null && config.hasPath("enableOcr")) {
      throw new StageException("enableOcr cannot be combined with tikaConfigPath; control OCR in the Tika config instead, "
          + "e.g. by excluding " + TesseractOCRParser.class.getName() + " from the DefaultParser");
    }
    if (globalTikaConfigSource != null && config.hasPath("enableOcr")) {
      throw new StageException("enableOcr cannot be combined with a global Tika config (" + globalTikaConfigSource
          + "); control OCR in the Tika config instead, e.g. by excluding " + TesseractOCRParser.class.getName()
          + " from the DefaultParser");
    }

    this.fileFetcher = FileContentFetcher.create(config);
  }

  // A supplied Tika config decides whether OCR runs; otherwise enableOcr does.
  private String describeOcrSetting() {
    if (tikaConfigPath != null) {
      return "as configured by tikaConfigPath";
    }
    if (globalTikaConfigSource != null) {
      return "as configured by " + globalTikaConfigSource;
    }
    return enableOcr ? "enabled" : "disabled";
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

    // we use an auto detect parser whether we are forking or not; ForkParser serializes it into the child JVMs
    tikaConfig = createTikaConfig();
    AutoDetectParser autoParser = new AutoDetectParser(tikaConfig);
    log.info("TextExtractor OCR: {}; tesseract binary on PATH: {}", describeOcrSetting(),
        tesseractDetected() ? "detected" : "not detected");

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
      if (!skipEmbeddedContentTypePrefixes.isEmpty()) {
        documentSelector = new EmbeddedContentTypeSelector(skipEmbeddedContentTypePrefixes);
      }
      if (parseTimeout != null) {
        // each worker is running in a single thread so we only need to run the extraction with a
        // single thread executor rather than using a thread pool.
        executorService = createParseExecutor();
      }
    }
  }

  /**
   * Builds the {@link ParseContext} for a single document. Each parse gets its own context because Tika
   * writes into it during a parse, and a parse abandoned after a timeout may still be running alongside
   * the next one. The context only holds references to objects built once in {@link #start()}, so this is cheap.
   */
  private ParseContext createParseContext() {
    ParseContext parseCtx = new ParseContext();
    // the fork case is left empty: the context is sent to the child JVM and TikaConfig/DocumentSelector
    // aren't Serializable
    if (!forkEnabled) {
      parseCtx.set(Parser.class, parser);
      // Without a TikaConfig in the context, Tika builds a fresh one (an expensive operation) for
      // every embedded document, so we set ours here to reuse it.
      parseCtx.set(TikaConfig.class, tikaConfig);
      // Skip parsing embedded documents whose content type matches a configured prefix. Only set when
      // configured, so the default (empty) leaves Tika's normal behavior unchanged.
      if (documentSelector != null) {
        parseCtx.set(DocumentSelector.class, documentSelector);
      }
    }
    return parseCtx;
  }

  /**
   * Creates the single-thread executor parses run on when parseTimeout is set. Its thread is a daemon so a
   * parse that ignores interruption after a timeout can't keep the JVM alive.
   */
  private static ExecutorService createParseExecutor() {
    return Executors.newSingleThreadExecutor(r -> {
      Thread thread = new Thread(r, "TextExtractor-parse-" + PARSE_THREAD_COUNT.incrementAndGet());
      thread.setDaemon(true);
      return thread;
    });
  }

  /**
   * Returns the {@link TikaConfig} used for parsing: from tikaConfigPath (cached, see {@link #getTikaConfig()}) or a
   * global Tika config when set, otherwise Tika's defaults, with {@link TesseractOCRParser} excluded unless enableOcr is true.
   */
  TikaConfig createTikaConfig() throws StageException {
    if (tikaConfigPath != null) {
      return getTikaConfig();
    }
    try {
      if (enableOcr || globalTikaConfigSource != null) {
        // Tika's default config loads the global Tika config when one is set
        return TikaConfig.getDefaultConfig();
      }
      return new TikaConfig(new ByteArrayInputStream(NO_OCR_TIKA_CONFIG.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new StageException("Error starting TextExtractor stage.", e);
    }
  }

  /**
   * Returns the {@link TikaConfig} for this stage's configured {@code tikaConfigPath}, building it at
   * most once per distinct config file across the JVM (see {@link #TIKA_CONFIG_CACHE}). Editing the file (changing its
   * last-modified time) causes it to be rebuilt on the next start.
   */
  private TikaConfig getTikaConfig() throws StageException {
    try {
      // Canonical paths used to prevent errors (if relative paths are used in a distributed environment, for example)
      File configFile = new File(tikaConfigPath).getCanonicalFile();
      // compared against the cached entry so an edited config file is rebuilt rather than served stale in a
      // long-lived JVM (e.g. the Admin API running several pipelines)
      long lastModified = configFile.lastModified();

      // compute so concurrent workers requesting the same config build it only once; a failed build
      // propagates out and leaves the existing entry untouched.
      return TIKA_CONFIG_CACHE.compute(configFile.getPath(), (path, cached) -> {
        if (cached != null && cached.getLeft() == lastModified) {
          return cached;
        }
        try {
          return Pair.of(lastModified, new TikaConfig(configFile));
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }).getRight();
    } catch (RuntimeException e) {
      throw new StageException("Error starting TextExtractor stage.", e.getCause() != null ? e.getCause() : e);
    } catch (Exception e) {
      throw new StageException("Error starting TextExtractor stage.", e);
    }
  }

  /**
   * Returns a description of the global Tika config that Tika's default constructors would load (the "tika.config"
   * system property, then the TIKA_CONFIG environment variable), or null if neither is set.
   */
  private static String detectGlobalTikaConfigSource() {
    String property = System.getProperty("tika.config");
    if (property != null) {
      return "tika.config system property";
    }
    return System.getenv("TIKA_CONFIG") != null ? "TIKA_CONFIG environment variable" : null;
  }

  private static boolean tesseractDetected() {
    try {
      return new TesseractOCRParser().hasTesseract();
    } catch (TikaConfigException e) {
      return false;
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

    Metadata metadata = createMetadataInput(doc);

    if (doc.has(byteArrayField)) {

      byte[] byteArray = doc.getBytes(byteArrayField);

      // wrap in a TikaInputStream so container-aware detectors (which need to spool the content to a file) can run;
      // a plain InputStream would limit detection to Mime-Magic on the leading bytes.
      try (TikaInputStream tikaStream = TikaInputStream.get(byteArray, metadata)) {
        parseInputStream(metadata, doc, tikaStream);
      } catch (IOException e) {
        log.warn("Error closing inputStream: ", e);
      }

    } else if (doc.has(filePathField)) {
      // get fileObject from path
      String filePath = doc.getString(filePathField);

      // wrap in a TikaInputStream so container-aware detectors (which need to spool the content to a file) can run;
      // a plain InputStream would limit detection to Mime-Magic on the leading bytes.
      try (InputStream fetchedStream = fileFetcher.getInputStream(filePath); TikaInputStream tikaStream = TikaInputStream.get(fetchedStream)) {
        parseInputStream(metadata, doc, tikaStream);
      } catch (IOException e) {
        log.warn("Error processing file {}", filePath, e);
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
   * Cleans the name of metadata field names to be in line with general standards for documents: trims, lowercases, and
   * replaces spaces, "-" and ":" (and "." when replaceDots is true) with "_".
   */
  static String cleanFieldName(String name, boolean replaceDots) {
    String cleanName = name.trim().toLowerCase().replace(' ', '_').replace('-', '_').replace(':', '_');
    return replaceDots ? cleanName.replace('.', '_') : cleanName;
  }

  /**
   * Parses the given input stream, extracts text content and metadata, and adds them to the provided document.
   *
   * @param metadata A {@code Metadata} object used to hold metadata extracted from the input stream.
   * @param doc A {@code Document} object where the extracted text data and metadata will be stored.
   * @param inputStream The {@code InputStream} containing the content to be parsed.
   */
  public void parseInputStream(Metadata metadata, Document doc, InputStream inputStream) throws StageException {
    ContentHandler bch = new BodyContentHandler(textContentLimit);
    ParseContext parseCtx = createParseContext();
    // the metadata written to the document; swapped for a fresh copy of the input hints on timeout
    Metadata metadataToWrite = metadata;
    if (forkEnabled || parseTimeout == null) {
      // fork path: StageException propagates on child failure; ForkParser's serverParseTimeoutMillis handles hangs.
      // non-fork path with no timeout: parse inline.
      parse(doc, inputStream, metadata, bch, parseCtx);
    } else {
      Future<?> future = executorService.submit(() -> {
        try {
          parse(doc, inputStream, metadata, bch, parseCtx);
        } catch (StageException e) {
          log.warn("Tika Exception: {}", e.getMessage());
        }
      });

      try {
        future.get(parseTimeout, TimeUnit.MILLISECONDS);
      } catch (TimeoutException e) {
        future.cancel(true);
        log.warn("Tika parsing timed out after {} ms", parseTimeout);
        // close the stream to abort a parse blocked on a stream read (the common hang case)
        try {
          inputStream.close();
        } catch (IOException ioe) {
          log.warn("Error closing input stream after parse timeout", ioe);
        }
        // Neither cancel() nor shutdownNow() can stop a parse that ignores interruption (e.g. stuck in a
        // CPU loop), so replace the executor to keep the next documents from queueing behind it. The
        // abandoned thread is a daemon and has its own ParseContext, so it can't keep the JVM alive or
        // interfere with later parses; it exits once the parse returns.
        executorService.shutdownNow();
        executorService = createParseExecutor();
        handleParseTimeout();
        // The parse thread may still be writing. The text is safe to read (BodyContentHandler writes to a
        // StringWriter, whose StringBuffer is synchronized), so the text extracted so far is kept. Metadata isn't
        // thread-safe, so the extracted metadata is dropped and only the input hints are rebuilt from the document.
        metadataToWrite = createMetadataInput(doc);
      } catch (Exception e) {
        log.warn("Error during async Tika parsing: {}", e.getMessage());
      }
    }

    doc.setOrAdd(textField, bch.toString());
    String newMetadataPrefix = metadataPrefix.isEmpty() ? "" : metadataPrefix + "_";
    for (String name : metadataToWrite.names()) {
      // clean the field name first.
      String cleanName = cleanFieldName(name, replaceDotsInMetadataNames);
      if (fieldFilter.shouldInclude(cleanName)) {
        String prefixedName = newMetadataPrefix + cleanName;
        for (String value : metadataToWrite.getValues(name)) {
          doc.setOrAdd(prefixedName, value);
        }
        if (fieldNamesField != null) {
          doc.setOrAdd(fieldNamesField, prefixedName);
        }
      }
    }
  }

  /**
   * Hook invoked when a parse attempt times out, after the input stream has been closed and the executor
   * discarded and recreated. No-op by default; subclasses can override it, e.g. to record timeout metrics.
   */
  protected void handleParseTimeout() {
  }

  /**
   * Clears the JVM-wide TikaConfig cache. Intended for tests.
   */
  static void clearTikaConfigCache() {
    TIKA_CONFIG_CACHE.clear();
  }

  private void parse(Document doc, InputStream inputStream, Metadata metadata, ContentHandler bch, ParseContext parseCtx)
      throws StageException {
    try {
      parser.parse(inputStream, bch, metadata, parseCtx);
    } catch (IOException | SAXException | TikaException e) {
      // We expect truncation when textContentLimit is reached.
      // It is best to check this condition because exceptions tend to be rewrapped. 
      if (WriteLimitReachedException.isWriteLimitReached(e)) {
        log.debug("textContentLimit ({}) reached; extracted text truncated.", textContentLimit);
      } else if (forkEnabled) {
        throw new StageException("Forked Tika process failed for document: " + doc.getId(), e);
      } else {
        log.warn("Tika Exception: {}", e.getMessage());
      }
    }
  }
}
