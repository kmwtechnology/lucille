package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.core.filehandler.FileHandlerException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Using a file path found on a Document, or a byte[] of its contents, applies file handlers to create children documents.
 * <p>
 * Config Parameters:
 * <ul>
 *   <li>
 *     fileHandlers (Map) : Specifies which file types should be handled / processed by this stage. Valid options include:
 *     <ul>
 *       <li>csv (Map, Optional) : For handling CSV files. Config will be passed to CSVFileHandler (if no alternate <code>class</code> is provided).</li>
 *       <li>json (Map, Optional) : For handling JSON or JSONL files. Config will be passed to JsonFileHandler (if no alternate <code>class</code> is provided).</li>
 *       <li>Custom <code>FileHandler</code> implementations.</li>
 *     </ul>
 *     <p> <b>Note:</b> An Exception is thrown if your <code>fileHandlers</code> Config is empty.
 *     <p> <b>Note:</b> See {@link FileHandler#createFromConfig(Config)} for notes on JSON and JSONL configuration.
 *     <p> <b>Note:</b> Lucille's XMLFileHandler is not supported, nor is any handler whose <code>supportsProcessFile()</code>
 *     returns false. The stage throws when it starts if one is configured.
 *   </li>
 *   <li>filePathField (String, Optional) : The field in your documents which will has the file path you want to apply handlers to.
 *   No processing will occur on documents without this field, even if they have the fileContentField present. Defaults to "file_path".</li>
 *   <li>fileContentField (String, Optional) : The field in your documents which has the file's contents as an array of bytes.
 *   When processing a document with a path to a supported file type, the handler will process the array of bytes found in this field,
 *   if present, before having the FileContentFetcher to open an InputStream for the file's contents. Defaults to "file_content".</li>
 *   <li>gcp (Map, Optional) : options for handling GoogleCloud files. Include if you are going to process documents with the filePathField set to a Google Cloud URI.
 *   See FileConnector for necessary arguments.</li>
 *   <li>s3 (Map, Optional) : options for handling S3 files. Include if you are going to process documents with the filePathField set to an S3 URI.
 *   See FileConnector for necessary arguments.</li>
 *   <li>azure (Map, Optional) : options for handling Azure files. Include if you are going to process documents with the filePathField set to an Azure URI.
 *   See FileConnector for necessary arguments.</li>
 *   <li>storageClients (Map, Optional) : custom StorageClients, keyed by URI scheme. Include if you are going to process documents with
 *   the filePathField set to a URI with one of those schemes. See FileConnector for necessary arguments.</li>
 * </ul>
 * <p>
 * The children follow the Iterator contract described on {@link FileHandler#processFile}, as with
 * {@link FileHandler#processFileAndPublish}: a record whose <code>next()</code> throws is logged and skipped, a null
 * from <code>next()</code> is skipped, and a <code>hasNext()</code> that throws stops the file and fails the parent
 * Document, along with any child already announced. Children produced before that have already been sent on.
 */
public class ApplyFileHandlers extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredParent("fileHandlers", new TypeReference<Map<String, Map<String, Object>>>(){})
      .include(FileConnector.STORAGE_SPEC)
      .optionalString("filePathField", "fileContentField").build();

  private static final Logger log = LoggerFactory.getLogger(ApplyFileHandlers.class);

  private final Config fileHandlersConfig;

  private final String filePathField;
  private final String fileContentField;
  private final FileContentFetcher fileFetcher;

  private Map<String, FileHandler> fileHandlers;



  /**
   * Creates the ApplyFileHandlers stage from the given config.
   * @param config Configuration for the ApplyFileHandlers stage.
   */
  public ApplyFileHandlers(Config config) {
    super(config);

    this.fileHandlersConfig = config.getConfig("fileHandlers");
    if (fileHandlersConfig.isEmpty()) {
      throw new IllegalArgumentException("Must specify at least one file handler.");
    }

    this.filePathField = ConfigUtils.getOrDefault(config, "filePathField", "file_path");
    this.fileContentField = ConfigUtils.getOrDefault(config, "fileContentField", "file_content");
    this.fileFetcher = FileContentFetcher.create(config);
  }

  @Override
  public void start() throws StageException {
    this.fileHandlers = FileHandler.createFromConfig(fileHandlersConfig);

    if (fileHandlers.isEmpty()) {
      throw new StageException("No file handlers could be created from the given fileHandlers.");
    }

    // this stage returns the handler's Iterator, so a handler that can only publish would fail on every file
    for (Map.Entry<String, FileHandler> entry : fileHandlers.entrySet()) {
      if (!entry.getValue().supportsProcessFile()) {
        throw new StageException("The FileHandler for \"" + entry.getKey() + "\" (" + entry.getValue().getClass().getName()
            + ") can only publish, and does not support processFile, which ApplyFileHandlers needs.");
      }
    }

    try {
      fileFetcher.start();
    } catch (IOException e) {
      throw new StageException("Error starting FileContentFetcher.", e);
    }
  }

  @Override
  public void stop() {
    fileFetcher.stop();
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(filePathField)) {
      return null;
    }

    String filePath = doc.getString(filePathField);
    // handler keys are lower case (see FileHandler.createFromConfig)
    String fileExtension = FilenameUtils.getExtension(filePath).toLowerCase(Locale.ROOT);

    if (!fileHandlers.containsKey(fileExtension)) {
      return null;
    }

    FileHandler handler = fileHandlers.get(fileExtension);
    InputStream fileContentStream;

    if (doc.has(fileContentField)) {
      byte[] fileContents = doc.getBytes(fileContentField);
      fileContentStream = new ByteArrayInputStream(fileContents);
    } else {
      try {
        fileContentStream = fileFetcher.getInputStream(filePath);
      } catch (IOException e) {
        throw new StageException("Could not fetch InputStream for file " + filePath, e);
      }
    }

    try {
      return new SkippingIterator(handler.processFile(fileContentStream, filePath), filePath);
    } catch (FileHandlerException | RuntimeException e) {
      // no Iterator will close the stream, so close it here: a stream from cloud storage holds an HTTP connection
      try {
        fileContentStream.close();
      } catch (IOException closeFailure) {
        e.addSuppressed(closeFailure);
      }
      throw new StageException("Could not process file " + filePath, e);
    }
  }

  /**
   * Wraps the Iterator that {@link FileHandler#processFile} returns, so that the handler's records are skipped or stop the
   * file as the Iterator contract says, before the Worker sees them.
   *
   * <p>This is needed because this stage returns the handler's Iterator to the Worker, and the Worker does not apply the
   * contract. It expects every element to be a Document, and fails the parent Document on any exception. A handler that
   * follows the contract can throw from <code>next()</code> for one bad record, or return null, and the Worker would fail
   * the parent or receive a null child. {@link FileHandler#processFileAndPublish} applies the contract itself, so a
   * handler behaves the same wherever it is used only if this stage applies it too.
   *
   * <p>What it does, per the contract (see {@link FileHandler#processFile}):
   * <ul>
   *   <li>A record whose <code>next()</code> throws a RuntimeException is logged and skipped, and so is a null.</li>
   *   <li>A <code>hasNext()</code> that throws means the file can't be read any further. It is rethrown as an
   *   IllegalStateException, which fails the parent. The wrapped Iterator has already closed its resources, as the
   *   contract requires; this class closes nothing.</li>
   * </ul>
   *
   * <p>This Iterator reads one record ahead: {@link #hasNext()} calls the wrapped <code>next()</code> to find the next
   * record that can be returned. It is used by one thread, and is not thread-safe.
   */
  private static final class SkippingIterator implements Iterator<Document> {

    private final Iterator<Document> records;
    private final String filePath;
    private Document pending = null;

    SkippingIterator(Iterator<Document> records, String filePath) {
      this.records = records;
      this.filePath = filePath;
    }

    @Override
    public boolean hasNext() {
      while (pending == null && hasNextRecord()) {
        try {
          pending = records.next();
        } catch (RuntimeException e) {
          // a single bad record: the iterator stays usable, so skip it and carry on
          log.error("Error reading next document from file {}", filePath, e);
        }
      }
      return pending != null;
    }

    // a hasNext() that throws means the file can't be read any further; the iterator has closed its resources
    private boolean hasNextRecord() {
      try {
        return records.hasNext();
      } catch (RuntimeException e) {
        throw new IllegalStateException("Error reading file " + filePath + ": " + e.getMessage(), e);
      }
    }

    @Override
    public Document next() {
      if (!hasNext()) {
        throw new NoSuchElementException("No more documents in file " + filePath);
      }
      Document doc = pending;
      pending = null;
      return doc;
    }
  }
}
