package com.kmwllc.lucille.message;

import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.KafkaDocument;
import com.typesafe.config.Config;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class HybridWorkerMessageManager implements WorkerMessageManager {

  public static final Logger log = LoggerFactory.getLogger(KafkaWorkerMessageManager.class);
  private final Consumer<String, String> sourceConsumer;
  private final LinkedBlockingQueue<KafkaDocument> pipelineDest;
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

  private final Config config;
  private final String pipelineName;

  public HybridWorkerMessageManager(Config config, String pipelineName,
                                    LinkedBlockingQueue<KafkaDocument> pipelineDest,
                                    LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
    this.config = config;
    this.pipelineName = pipelineName;
    this.pipelineDest = pipelineDest;
    this.offsets = offsets;

    Properties consumerProps = KafkaUtils.createConsumerProps(config);

    // append random string to kafka client ID to prevent kafka from issuing a warning when multiple consumers
    // with the same client ID are started in separate worker threads
    String kafkaClientId = "lucille-worker-" + pipelineName + "-" + RandomStringUtils.randomAlphanumeric(8);
    consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, kafkaClientId);
    this.sourceConsumer = new KafkaConsumer(consumerProps);
    this.sourceConsumer.subscribe(Collections.singletonList(KafkaUtils.getSourceTopicName(pipelineName)));
  }

  /**
   * Polls for a document that is waiting to be processed by the pipeline.
   *
   * Does not commit offsets.
   */
  @Override
  public KafkaDocument pollDocToProcess() throws Exception {
    ConsumerRecords<String, String> consumerRecords = sourceConsumer.poll(KafkaUtils.POLL_INTERVAL);
    KafkaUtils.validateAtMostOneRecord(consumerRecords);
    if (consumerRecords.count() > 0) {
      ConsumerRecord<String, String> record = consumerRecords.iterator().next();
      return new KafkaDocument(record);
    }
    return null;
  }

  @Override
  public void commitPendingDocOffsets() throws Exception {
    Map<TopicPartition,OffsetAndMetadata> batchOffsets = null;
    while ((batchOffsets = offsets.poll()) != null) {
      sourceConsumer.commitSync(batchOffsets);
    }
  }

  /**
   * Sends a processed document to the appropriate destination for documents waiting to be indexed.
   *
   */
  @Override
  public void sendCompleted(Document document) throws Exception {
    pipelineDest.put((KafkaDocument)document); //TODO
  }

  @Override
  public void sendFailed(Document document) throws Exception {
  }

  @Override
  public void sendEvent(Event event) throws Exception {
  }

  @Override
  public void close() throws Exception {
    sourceConsumer.close();
  }

}

