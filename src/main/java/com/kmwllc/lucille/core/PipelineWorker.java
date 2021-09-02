package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class PipelineWorker {

  public static final Logger log = LoggerFactory.getLogger(PipelineWorker.class);
  public static final int TIMEOUT_CHECK_MS = 1000;

  final Duration pollInterval;
  final int maxRetries;

  private final Config config;
  private final Pipeline pipeline;
  public final CuratorFramework curatorFramework;
  private final Consumer<String, String> kafkaConsumer;
  private final KafkaProducer kafkaProducer;
  private final AtomicReference<Instant> pollInstant = new AtomicReference();
  private final String retryCounterPrefix;

  public PipelineWorker(Config config, String pipelineName) throws Exception {
    log.info("Initializing Lucille Pipeline Worker");
    this.config = config;
    pollInstant.set(Instant.now());
    pollInterval = Duration.ofMillis(config.getLong("kafka.pollIntervalMs"));
    maxRetries = config.getInt("worker.maxRetries");

    retryCounterPrefix = "/LucilleCounters/" + config.getString("kafka.consumerGroupId") + "/"
      + config.getString("kafka.sourceTopic") + "/";

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    curatorFramework = CuratorFrameworkFactory.newClient(config.getString("zookeeper.connectString"), retryPolicy);
    curatorFramework.start();

    Properties consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, config.getString("kafka.consumerGroupId"));
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 1000 * config.getInt("kafka.maxPollIntervalSecs"));
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,  StringDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

    kafkaConsumer = new KafkaConsumer(consumerProps);
    kafkaConsumer.subscribe(Collections.singletonList(config.getString("kafka.sourceTopic")));

    Properties producerProps = new Properties();
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getString("kafka.bootstrapServers"));
    producerProps.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, config.getInt("kafka.maxRequestSize"));
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    kafkaProducer = new KafkaProducer(producerProps);
    pipeline = Pipeline.fromConfig(config, pipelineName);
  }

  public static void main(String[] args) throws Exception {

    Config config = ConfigAccessor.loadConfig();
    PipelineWorker worker = new PipelineWorker(config, args.length > 0 ? args[0] : "pipeline1");

    int maxProcessingSecs = config.getInt("worker.maxProcessingSecs");
    spawnWatcher(worker, maxProcessingSecs);

    try {
      worker.run();
    } finally {
      worker.close();
    }

  }

  public void run() throws Exception {
    while (true) {

      // STEP 1: Poll Kafka for a new message to process
      pollInstant.set(Instant.now());
      ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(pollInterval);

      if (consumerRecords.count() == 0) {
        continue;
      } else if (consumerRecords.count() > 1) {
        throw new Exception("Kafka poll returned more than 1 message but this shouldn't happen");
      }

      ConsumerRecord<String, String> record = consumerRecords.iterator().next();

      // STEP 2: Update the shared counter for the message
      String counterPath = retryCounterPrefix + record.key() + "_" + record.partition() + "_" + record.offset();
      int retryCount;
      try (SharedCount counter = new SharedCount(curatorFramework, counterPath, 0)) {
        counter.start();
        retryCount = counter.getCount();
        counter.setCount(++retryCount);
      }

      // STEP 3: Send the message to the DLQ if the retry count is exceeded
      if (retryCount > maxRetries) {
        if (config.hasPath("kafka.failureTopic")) {
          ProducerRecord<String, String> producerRecord =
            new ProducerRecord(config.getString("kafka.failureTopic"), record.key(), record.value());
          RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
          kafkaProducer.flush();
          log.info("Sent to DLQ: " + record.key() + " (partition: " + metadata.partition() + ")");
        }

        try {
          kafkaConsumer.commitSync(); // could fail if consumer has been evicted from group
        } catch (Exception e) {
          log.error("Couldn't commit kafka consumer offset",e);
        }
        curatorFramework.delete().quietly().deletingChildrenIfNeeded().forPath(counterPath);
        continue;
      }

      // STEP 4: Run the message through the pipeline
      List<Document> results = pipeline.processKafkaJsonMessage(record);

      // Step 5: Send the results to the destination Kafka topic and flush
      if (results==null || results.isEmpty()) {
        log.warn("No results.");
      } else {
        for (Document document : results) {
          ProducerRecord<String, String> producerRecord =
            new ProducerRecord(config.getString("kafka.destTopic"), document.getId(), document.toString());
          RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(producerRecord).get();
          log.info("Sent to Kafka: " + document.getId() + " (partition: " + metadata.partition() + ")");
        }
        kafkaProducer.flush();
      }

      // Step 6: Commit the updated Kafka consumer offset
      try {
        kafkaConsumer.commitSync();
      } catch (Exception e) {
        log.error("Failed to commit Kafka consumer offset", e);
      }

      // Step 7: Delete the retry counter for the message
      curatorFramework.delete().quietly().deletingChildrenIfNeeded().forPath(counterPath);
    }
  }

  public void close() {
    kafkaProducer.close();
    kafkaConsumer.close();
    curatorFramework.close();
  }

  public AtomicReference<Instant> getPreviousPollInstant() {
    return pollInstant;
  }

  private static void spawnWatcher(PipelineWorker worker, int maxProcessingSecs) {
    Executors.newSingleThreadExecutor().submit(new Runnable() {
      public void run() {
        while (true) {
          if (Duration.between(worker.getPreviousPollInstant().get(),Instant.now()).getSeconds() > maxProcessingSecs) {
            log.error("Shutting down because maximum allowed time between previous poll is exceeded.");
            System.exit(1);
          }
          try {
            Thread.sleep(TIMEOUT_CHECK_MS);
          } catch (InterruptedException e) {
            log.error("Watcher thread interrupted");
            return;
          }
        }
      }
    });
  }


}
