package com.kmwllc.lucille.producer;

import com.kmwllc.lucille.producer.datatype.FileInfo;
import com.kmwllc.lucille.producer.datatype.FileInfoSerializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
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
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

public class FileTraverser extends SimpleFileVisitor<Path> implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(FileTraverser.class);
  private final Path path;
  private final Pattern regex;
  // TODO: truncate vs ignore file if too large
  public static final int MAX_FILE_SIZE_KB = 200;
  private final Producer<String, FileInfo> producer;
  private final boolean excludeFileData;
  private final Integer maxDepth;
  private final String topic;

  public FileTraverser(String path, String topic, String location, String regex, boolean excludeFileData,
                       String maxDepth) {
    this.path = Path.of(path);
    if (!Files.exists(this.path)) {
      throw new IllegalArgumentException("Provided path does not exist");
    }
    this.topic = topic;
    this.regex = regex == null ? null : Pattern.compile(regex);
    this.excludeFileData = excludeFileData;
    this.maxDepth = maxDepth == null ? null : Integer.parseInt(maxDepth);
    Properties props = new Properties();
    props.putIfAbsent(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, location);
    props.putIfAbsent(ProducerConfig.CLIENT_ID_CONFIG, "LucilleFileTraverser");
    props.putIfAbsent(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    props.putIfAbsent(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, FileInfoSerializer.class.getName());
    producer = new KafkaProducer<>(props);
  }

  FileTraverser(String path, String topic, String regex, boolean excludeFileData, String maxDepth,
                Producer<String, FileInfo> producer) {
    this.path = Path.of(path);
    if (!Files.exists(this.path)) {
      throw new IllegalArgumentException("Provided path does not exist");
    }
    this.topic = topic;
    this.regex = regex == null ? null : Pattern.compile(regex);
    this.excludeFileData = excludeFileData;
    this.maxDepth = maxDepth == null ? null : Integer.parseInt(maxDepth);
    this.producer = producer;
  }

  public static void main(String[] args) throws ParseException, IOException {
    Options cliOptions = new Options()
        .addOption(Option.builder("p").required().argName("PATH").longOpt("path").numberOfArgs(1)
            .desc("Path to the file or directory").build())
        // TODO: should kafka info be provided in a properties file?
        .addOption(Option.builder("t").required().argName("TOPIC").longOpt("topic-name").numberOfArgs(1)
            .desc("Name of the topic the file should be sent to").build())
        .addOption(Option.builder("l").required().argName("LOCATION").longOpt("location")
            .numberOfArgs(1).desc("The location of the Kafka endpoint").build())
        .addOption(Option.builder("i").argName("INCLUDE").longOpt("include").numberOfArgs(1)
            .desc("A regex describing file names to ignore").build())
        .addOption(Option.builder("e").longOpt("exclude-data")
            .desc("Does not include file with Kafka message, only file information is provided").build())
        .addOption(Option.builder("d").argName("DEPTH").longOpt("max-depth").numberOfArgs(1)
            .desc("The max depth that should be traversed").build())
        .addOption(Option.builder("h").longOpt("help")
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

    try (final FileTraverser traverser = new FileTraverser(cli.getOptionValue('p'), cli.getOptionValue('t'),
        cli.getOptionValue('l'), cli.getOptionValue('i'), cli.hasOption('e'), cli.getOptionValue('m'))) {
      traverser.walkTree();
    }
  }

  @Override
  public void close() {
    producer.close();
  }

  public void walkTree() {
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
    log.info("Finished walking file tree");
  }

  // TODO: do we want to use regex on directories too? should the pattern only match the file name?

  @Override
  public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
    if (regex != null && regex.matcher(file.toString()).matches()) {
      log.debug("Skipping file because of regex {}", file);
      return FileVisitResult.CONTINUE;
    }
    if (attrs.size() > MAX_FILE_SIZE_KB * 1000) {
      // TODO: do we want to send file info even if file is too big?
      log.debug("File size greater than max size");
      return FileVisitResult.CONTINUE;
    }
    log.debug("Visiting file {}", file);

    final FileInfo info;
    try {
      info = new FileInfo(file, attrs, !excludeFileData);
    } catch (IOException e) {
      log.error("Could not process file {}", file, e);
      return FileVisitResult.CONTINUE;
    }

    ProducerRecord<String, FileInfo> record = new ProducerRecord<>(topic, info.getId(), info);
    producer.send(record);

    return FileVisitResult.CONTINUE;
  }

  @Override
  public FileVisitResult visitFileFailed(Path file, IOException exc) {
    log.error("Error occurred while visiting file {}", file, exc);
    return FileVisitResult.CONTINUE;
  }
}
