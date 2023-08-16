package com.kmwllc.lucille.filetraverser;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.filetraverser.data.kafkaserde.DocumentDeserializer;
import com.kmwllc.lucille.filetraverser.data.producer.DefaultDocumentProducer;
import org.apache.commons.cli.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsoleFileCopier implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(ConsoleFileCopier.class);
  private final Path path;
  private final boolean fromBeginning;
  private final Consumer<String, Document> consumer;
  private final Duration pollTimeout = Duration.ofMillis(1000);

  public ConsoleFileCopier(String path, String topic, String location, boolean fromBeginning) {
    this.path = path == null ? null : Path.of(path);
    this.fromBeginning = fromBeginning;
    Properties props = new Properties();
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, location);
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "ConsoleFileConsumer");
    props.putIfAbsent(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DocumentDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public static void main(String[] args) throws Exception {
    Options cliOptions =
        new Options()
            // TODO: should kafka info be provided in a properties file?
            .addOption(
                Option.builder("t")
                    .required()
                    .argName("TOPIC")
                    .longOpt("topic-name")
                    .numberOfArgs(1)
                    .desc("Name of the topic the file should be sent to")
                    .build())
            .addOption(
                Option.builder("l")
                    .required()
                    .argName("LOCATION")
                    .longOpt("location")
                    .numberOfArgs(1)
                    .desc("The location of the Kafka endpoint")
                    .build())
            .addOption(
                Option.builder("b")
                    .longOpt("from-beginning")
                    .desc("Whether the consumer should read the topic from the beginning")
                    .build())
            .addOption(
                Option.builder("p")
                    .argName("PATH")
                    .longOpt("path")
                    .numberOfArgs(1)
                    .desc(
                        "Path to the directory files should be output; if omitted, file contents are not saved")
                    .build());

    CommandLine cli = null;
    try {
      cli = new DefaultParser().parse(cliOptions, args);
    } catch (UnrecognizedOptionException | MissingOptionException e) {
      try (StringWriter writer = new StringWriter();
          PrintWriter printer = new PrintWriter(writer)) {

        String header =
            "Read FileInfo objects from the given Kafka topic and write information to the "
                + "console and (optionally) disk.";
        new HelpFormatter()
            .printHelp(printer, 256, "ConsoleFileConsumer", header, cliOptions, 2, 10, "", true);
        log.info(writer.toString());
      }
      System.exit(1);
    }

    try (ConsoleFileCopier consoleFileCopier =
        new ConsoleFileCopier(
            cli.hasOption('p') ? cli.getOptionValue('p') : null,
            cli.getOptionValue('t'),
            cli.getOptionValue('l'),
            cli.hasOption('b'))) {
      consoleFileCopier.readFiles();
    }
  }

  @Override
  public void close() {
    consumer.close();
  }

  @SuppressWarnings("InfiniteLoopStatement")
  public void readFiles() throws IOException {
    if (path != null) {
      Files.createDirectories(path);
    }

    if (fromBeginning) {
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());
    }

    while (true) {
      ConsumerRecords<String, Document> records = consumer.poll(pollTimeout);
      if (!records.isEmpty()) {
        log.info("{} records received", records.count());
        for (ConsumerRecord<String, Document> record : records) {
          log.info("Info received [{}]", record.value().toString());
          if (path != null && record.value().has(DefaultDocumentProducer.CONTENT)) {
            log.debug("Writing file to disk");
            // TODO: what happens with absolute path?
            Path file =
                Path.of(path.toString(), record.value().getString(FileTraverser.FILE_PATH))
                    .normalize();
            Files.createDirectories(file.getParent());
            Files.write(file, DefaultDocumentProducer.decodeFileContents(record.value()));
          }
        }
        consumer.commitAsync();
      } else {
        log.info("No records received");
      }
    }
  }
}
