package com.kmwllc.lucille.producer;

import com.kmwllc.lucille.producer.datatype.FileInfo;
import com.kmwllc.lucille.producer.datatype.FileInfoDeserializer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.MissingOptionException;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.UnrecognizedOptionException;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
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

public class ConsoleFileConsumer implements AutoCloseable {
  private static final Logger log = LogManager.getLogger(ConsoleFileConsumer.class);
  private final Path path;
  private final boolean fromBeginning;
  private final Consumer<String, FileInfo> consumer;
  private final Duration pollTimeout = Duration.ofMillis(1000);

  public ConsoleFileConsumer(String path, String topic, String location, boolean fromBegining) {
    this.path = path == null ? null : Path.of(path);
    this.fromBeginning = fromBegining;
    Properties props = new Properties();
    props.putIfAbsent(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, location);
    props.putIfAbsent(ConsumerConfig.GROUP_ID_CONFIG, "ConsoleFileConsumer");
    props.putIfAbsent(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, FileInfoDeserializer.class.getName());
    props.putIfAbsent(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.putIfAbsent(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 16000);
    consumer = new KafkaConsumer<>(props);
    consumer.subscribe(Collections.singletonList(topic));
  }

  public static void main(String[] args) throws Exception {
    OptionGroup group = new OptionGroup()
        .addOption(Option.builder("e").longOpt("exclude-data")
        .desc("Does not include file with Kafka message, only file information is provided and printed to the console (default)")
            .build())
        .addOption(Option.builder("p").argName("PATH").longOpt("path").numberOfArgs(1)
            .desc("Path to the directory files should be output").build());
    Options cliOptions = new Options()
        // TODO: should kafka info be provided in a properties file?
        .addOption(Option.builder("t").required().argName("TOPIC").longOpt("topic-name").numberOfArgs(1)
            .desc("Name of the topic the file should be sent to").build())
        .addOption(Option.builder("l").required().argName("LOCATION").longOpt("location")
            .numberOfArgs(1).desc("The location of the Kafka endpoint").build())
        .addOption(Option.builder("b").longOpt("from-beginning")
            .desc("Whether the consumer should read the topic from the beginning").build())
        .addOptionGroup(group);

    CommandLine cli = null;
    try {
      cli = new DefaultParser().parse(cliOptions, args);
    } catch (UnrecognizedOptionException | MissingOptionException e) {
      try (StringWriter writer = new StringWriter();
           PrintWriter printer = new PrintWriter(writer)) {

        String header = "Read FileInfo objects from the given Kafka topic and write information to the " +
            "console and (optionally) disk.";
        new HelpFormatter().printHelp(printer, 256, "ConsoleFileConsumer", header, cliOptions,
            2, 10, "", true);
        log.info(writer.toString());
      }
      System.exit(1);
    }

    try (ConsoleFileConsumer consoleFileConsumer = new ConsoleFileConsumer(cli.hasOption('p') ? null : cli.getOptionValue('p'),
        cli.getOptionValue('t'), cli.getOptionValue('l'), cli.hasOption('b'))) {
      consoleFileConsumer.readFiles();
    }
  }

  @Override
  public void close() {
    consumer.close();
  }

  public void readFiles() throws IOException {
    if (path != null) {
      Files.createDirectories(path);
    }

    if (fromBeginning) {
      consumer.poll(0);
      consumer.seekToBeginning(consumer.assignment());
    }

    while (true) {
      ConsumerRecords<String, FileInfo> records = consumer.poll(pollTimeout);
      if (!records.isEmpty()) {
        log.info("{} records received", records.count());
        for (ConsumerRecord<String, FileInfo> record : records) {
          log.info("Info received [{}]", record.value().toString());
          if (path != null && record.value().hasFileContent()) {
            log.debug("Writing file to disk");
            // TODO: what happens with absolute path?
            Path file = path.relativize(Path.of(record.value().getFilePath()));
            Files.createDirectories(file.getParent());
            Files.write(file, record.value().getFileContent());
          }
        }
      } else {
        log.info("No records received");
      }
    }
  }
}
