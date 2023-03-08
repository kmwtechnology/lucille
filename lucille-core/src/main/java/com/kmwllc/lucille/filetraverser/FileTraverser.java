package com.kmwllc.lucille.filetraverser;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.filetraverser.data.DocumentProducer;
import com.kmwllc.lucille.filetraverser.data.kafkaserde.DocumentSerializer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class FileTraverser extends SimpleFileVisitor<Path> implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(FileTraverser.class);
  public static final String FILE_PATH = "file_path";
  public static final String MODIFIED = "file_modification_date";
  public static final String CREATED = "file_creation_date";
  public static final String SIZE = "file_size_bytes";
  public static int MAX_FILE_SIZE_BYTES = 250 * 1_000_000;
  private final List<Path> paths;
  private final List<Pattern> includes;
  private final List<Pattern> excludes;
  private final Producer<String, Document> producer;
  private final Integer maxDepth;
  private final String topic;
  private final DocumentProducer docProducer;
  private final boolean binaryData;

  public FileTraverser(String[] paths, String topic, String brokerList, String[] includeRegex, String[] excludeRegex,
                       boolean binaryData, String dataType, String childCopyParentMetadata) {
    this.binaryData = binaryData;

    // Turn all provided paths into Path objects, replacing the "~" character with the user.home system property
    this.paths = Arrays.stream(paths).map(path ->
      Path.of(path.replace("~", System.getProperty("user.home"))).toAbsolutePath().normalize())
      .collect(Collectors.toList());

    // Require that provided paths exist
    if (!this.paths.stream().allMatch(Files::exists)) {
      throw new IllegalArgumentException("Provided path does not exist");
    }

    this.topic = topic;

    // Compile include and exclude regex paths or set an empty list if none were provided (allow all files)
    this.includes = includeRegex == null
        ? Collections.emptyList()
        : Arrays.stream(includeRegex).map(Pattern::compile).collect(Collectors.toList());
    this.excludes = excludeRegex == null
        ? Collections.emptyList()
        : Arrays.stream(excludeRegex).map(Pattern::compile).collect(Collectors.toList());

    this.maxDepth = null;

    // Instantiate the desired doc producer
    this.docProducer = DocumentProducer.getProducer(dataType, Boolean.parseBoolean(childCopyParentMetadata));

    // Set up the KafkaProducer
    Properties props = new Properties();
    props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
    props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "LucilleFileTraverser");
    props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, DocumentSerializer.class.getName());
    props.putIfAbsent(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, MAX_FILE_SIZE_BYTES);
    producer = new KafkaProducer<>(props);
  }

  public static void main(String[] args) throws ParseException, IOException {
    Options cliOptions = new Options()
        .addOption(Option.builder("p").required().argName("PATH").longOpt("path").hasArgs()
            .desc("Path to the file or directory").build())
        // TODO: should kafka info be provided in a properties file?
        .addOption(Option.builder("t").required().argName("TOPIC").longOpt("topic-name").hasArgs()
            .desc("Name of the topic the file should be sent to").build())
        .addOption(Option.builder("l").required().argName("BROKER_LIST").longOpt("broker-list").hasArg()
            .numberOfArgs(1).desc("The location of the Kafka endpoint").build())
        .addOption(Option.builder("i").argName("INCLUDE").longOpt("include").hasArgs()
            .desc("A regex describing file names to include. --exclude takes precedence on conflict. If left empty," +
                " everything is matched. File must match any regex to be included.").build())
        .addOption(Option.builder("e").argName("EXCLUDE").longOpt("exclude").hasArgs()
            .desc("A regex describing file names to exclude. If left empty, nothing is matched. File must match any regex " +
                "to be included.").build())
        .addOption(Option.builder("b").longOpt("binary-data").hasArg(false)
            .desc("Includes file data in Kafka message").build())
        .addOption(Option.builder("d").longOpt("data-type").hasArg(true)
            .desc("The data type that the file should be parsed as (default is 'default')").build())
        .addOption(Option.builder().longOpt("copy-parent-metadata").hasArg(true)
            .desc("When child documents are being produced, should the child copy all of parent's metadata").build())
        .addOption(Option.builder("h").longOpt("help").hasArg(false)
            .desc("This help message").build());

    CommandLine cli = null;
    try {
      cli = new DefaultParser().parse(cliOptions, args);
    } catch (UnrecognizedOptionException | MissingOptionException e) {
      try (StringWriter writer = new StringWriter();
           PrintWriter printer = new PrintWriter(writer)) {

        String header = "Walk file tree and send files with info to a given Kafka topic";
        new HelpFormatter().printHelp(printer, 256, "FileTraverser", header, cliOptions,
            2, 10, "", true);
        log.info(writer.toString());
      }
      System.exit(1);
    }

    try (final FileTraverser traverser = new FileTraverser(
        cli.getOptionValues("path"),
        cli.getOptionValue("topic-name"),
        cli.getOptionValue("broker-list"),
        cli.getOptionValues("include"),
        cli.getOptionValues("exclude"),
        cli.hasOption("binary-data"),
        cli.getOptionValue("data-type", "DEFAULT"),
        cli.getOptionValue("copy-parent-metadata", "true"))) {
      traverser.walkTree();
    }
  }

  @Override
  public void close() {
    producer.close();
  }

  /**
   * Walks the file tree using {@link Files#walkFileTree(Path, FileVisitor)}, or
   * {@link Files#walk(Path, int, FileVisitOption...)} if a max depth is set.
   */
  public void walkTree() {
    log.info("Waking provided paths: {}", paths);
    for (Path path : paths) {
      log.info("Walking file tree at {}", path);
      try {
        if (maxDepth == null) {
          Files.walkFileTree(path, this);
        } else {
          Files.walkFileTree(path, Set.of(), maxDepth, this);
        }
      } catch (IOException e) {
        log.fatal("Fatal exception occurred while walking file tree", e);
      }
      log.info("Finished walking file tree at {}", path);
    }
    log.info("Done walking provided paths");
  }

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
    String fileName = file.toString();

    // Skip file if any exclude regex patterns match or if no include regex patterns match
    if (excludes.parallelStream().anyMatch(pattern -> pattern.matcher(fileName).matches())
    || (!includes.isEmpty() && includes.parallelStream().noneMatch(pattern -> pattern.matcher(fileName).matches()))) {
      log.debug("Skipping file because of include or exclude regex");
      return FileVisitResult.CONTINUE;
    }

    log.debug("Visiting file {}", file);

    final String id = docProducer.createId(file.toString());
    final Document baseDoc = Document.create(id);

    // Set up basic file properties on the doc
    baseDoc.setField(FILE_PATH, fileName);
    baseDoc.setField(MODIFIED, attrs.lastModifiedTime().toInstant().toString());
    baseDoc.setField(CREATED, attrs.creationTime().toInstant().toString());
    baseDoc.setField(SIZE, attrs.size());

    if (binaryData) {
      // Make sure the file passes the file size check if we're sending binary data and should do the file size check
      if (attrs.size() > MAX_FILE_SIZE_BYTES && docProducer.shouldDoFileSizeCheck()) {
        handleFailure(file, null, new IOException("File binary data larger than max configured size"));
      } else {
        try {
          sendDocumentsToTopic(docProducer.produceDocuments(file, baseDoc));
        } catch (DocumentException | IOException e) {
          handleFailure(file, baseDoc, e);
        }
      }
    } else {
      sendDocumentToTopic(baseDoc);
    }

    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) {
    handleFailure(file, null, exc);

    return FileVisitResult.CONTINUE;
  }

  /**
   * Send a tombstone for the given file. Calls {@link DocumentProducer#createTombstone(Path, Document, Throwable)} to
   * update the given {@link Document} with error info or create a new document if it's null. After the {@code Document}
   * has been updated, {@link this#sendDocumentsToTopic(List)} sends the document.
   *
   * @param file The file the error occurred on
   * @param doc A null document if no information has been extracted yet, or an existing doc to add error information to
   * @param exception The exception that occurred
   */
  private void handleFailure(Path file, Document doc, Throwable exception) {
    log.error("Error occurred while visiting file {}", file, exception);

    try {
      sendDocumentToTopic(docProducer.createTombstone(file, doc, exception));
    } catch (DocumentException e) {
      log.error("Error occurred while sending document tombstone", e);
    }
  }

  private void sendDocumentToTopic(Document doc) {
    ProducerRecord<String, Document> record = new ProducerRecord<>(topic, doc.getId(), doc);
    producer.send(record);
  }

  private void sendDocumentsToTopic(List<Document> docs) {
    for (Document doc : docs) {
      sendDocumentToTopic(doc);
    }
  }
}
