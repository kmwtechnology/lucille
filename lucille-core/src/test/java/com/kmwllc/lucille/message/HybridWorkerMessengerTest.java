package com.kmwllc.lucille.message;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

public class HybridWorkerMessengerTest {

  private final TopicPartition a0 = new TopicPartition("a", 0);
  private final TopicPartition a1 = new TopicPartition("a", 1);
  private final TopicPartition b0 = new TopicPartition("b", 0);

  private KafkaConsumer consumer;
  private LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;
  private HybridWorkerMessenger messenger;

  @Before
  public void setUp() {
    Config config = ConfigFactory.parseString("kafka.events: false");
    consumer = mock(KafkaConsumer.class);
    offsets = new LinkedBlockingQueue<>();
    messenger = new HybridWorkerMessenger(config, "pipeline1", new LinkedBlockingQueue<Document>(), offsets, consumer);
  }

  @Test
  public void testEmptyQueueDoesNotCommit() throws Exception {
    messenger.commitPendingDocOffsets();
    verify(consumer, never()).commitSync(anyMap());
  }

  @Test
  public void testSingleMapCommittedAsIs() throws Exception {
    Map<TopicPartition, OffsetAndMetadata> m = Map.of(a0, new OffsetAndMetadata(5));
    offsets.add(m);
    messenger.commitPendingDocOffsets();
    verify(consumer, times(1)).commitSync(m);
    assertTrue(offsets.isEmpty());
  }

  @Test
  public void testSamePartitionOutOfOrderKeepsMax() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(10)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(4)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(7)));
    messenger.commitPendingDocOffsets();

    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
    verify(consumer, times(1)).commitSync(captor.capture());
    assertEquals(Map.of(a0, new OffsetAndMetadata(10)), captor.getValue());
  }

  @Test
  public void testMultiplePartitionsAndTopics() throws Exception {
    offsets.add(Map.of(a0, new OffsetAndMetadata(3), a1, new OffsetAndMetadata(9)));
    offsets.add(Map.of(a0, new OffsetAndMetadata(8), b0, new OffsetAndMetadata(2)));
    offsets.add(Map.of(a1, new OffsetAndMetadata(1), b0, new OffsetAndMetadata(6)));
    messenger.commitPendingDocOffsets();

    ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> captor = ArgumentCaptor.forClass(Map.class);
    verify(consumer, times(1)).commitSync(captor.capture());
    assertEquals(Map.of(a0, new OffsetAndMetadata(8), a1, new OffsetAndMetadata(9), b0, new OffsetAndMetadata(6)),
        captor.getValue());
  }
}
