package com.kmwllc.lucille.producer;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.producer.data.DocumentProducer;
import com.kmwllc.lucille.producer.data.kafkaserde.DocumentSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.UnrecognizedOptionException;
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
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
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
  private final List<String> topics;
  private final DocumentProducer docProducer;
  private final boolean binaryData;

  // TODO: remove multiple topic functionality
  public FileTraverser(String[] paths, String[] topics, String brokerList, String[] includeRegex, String[] excludeRegex,
                       boolean binaryData, String dataType) {
    this.binaryData = binaryData;
    this.paths = Arrays.stream(paths).map(path -> path.replace("~", System.getProperty("user.home")))
        .map(Path::of).collect(Collectors.toList());
    if (!this.paths.stream().allMatch(Files::exists)) {
      throw new IllegalArgumentException("Provided path does not exist");
    }
    this.topics = Arrays.asList(topics);
    this.includes = includeRegex == null
        ? Collections.emptyList()
        : Arrays.stream(includeRegex).map(Pattern::compile).collect(Collectors.toList());
    this.excludes = excludeRegex == null
        ? Collections.emptyList()
        : Arrays.stream(excludeRegex).map(Pattern::compile).collect(Collectors.toList());
    this.maxDepth = null;
    this.docProducer = DocumentProducer.getProducer(dataType);
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
        cli.getOptionValues("topic-name"),
        cli.getOptionValue("broker-list"),
        cli.getOptionValues("include"),
        cli.getOptionValues("exclude"),
        cli.hasOption("binary-data"),
        cli.getOptionValue("data-type", "DEFAULT"))) {
      traverser.walkTree();
    }
  }

  @Override
  public void close() {
    producer.close();
  }

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
    if (excludes.parallelStream().anyMatch(pattern -> pattern.matcher(fileName).matches())
    || (!includes.isEmpty() && includes.parallelStream().noneMatch(pattern -> pattern.matcher(fileName).matches()))) {
      log.debug("Skipping file because of include or exclude regex");
      return FileVisitResult.CONTINUE;
    }

    // TODO: do docProducer size check
    if (attrs.size() > MAX_FILE_SIZE_BYTES && binaryData) {
      handleFailure(file, new IOException("File binary data larger than max configured size"));
      return FileVisitResult.CONTINUE;
    }

    log.debug("Visiting file {}", file);

    final String id = docProducer.createId(file.toString());
    final Document baseDoc;
    try {
      baseDoc = new Document(id);
    } catch (DocumentException e) {
      handleFailure(file, e);
      return FileVisitResult.CONTINUE;
    }

    // TODO: ability to turn copying on and off for child documents (metadata flag) no clone in that case
    baseDoc.setField(FILE_PATH, fileName);
    baseDoc.setField(MODIFIED, attrs.lastModifiedTime().toInstant().toString());
    baseDoc.setField(CREATED, attrs.creationTime().toInstant().toString());
    baseDoc.setField(SIZE, attrs.size());
    if (binaryData) {
      try {
        sendDocumentsToTopics(docProducer.produceDocuments(file, baseDoc));
      } catch (DocumentException | IOException e) {
        handleFailure(file, e);
      }
    } else {
      sendDocumentToTopics(baseDoc);
    }

    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) {
    handleFailure(file, exc);

    return FileVisitResult.CONTINUE;
  }

  private void handleFailure(Path file, Throwable exception) {
    log.error("Error occurred while visiting file {}", file, exception);

    try {
      sendDocumentToTopics(docProducer.createTombstone(file, exception));
    } catch (DocumentException e) {
      log.error("Error occurred while sending document tombstone", e);
    }
  }

  private void sendDocumentToTopics(Document doc) {
    for (String topic : topics) {
      ProducerRecord<String, Document> record = new ProducerRecord<>(topic, doc.getId(), doc);
      producer.send(record);
    }
  }

  private void sendDocumentsToTopics(List<Document> docs) {
    for (Document doc : docs) {
      sendDocumentToTopics(doc);
    }
  }
}
