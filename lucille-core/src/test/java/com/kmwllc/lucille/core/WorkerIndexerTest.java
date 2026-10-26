package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import com.kmwllc.lucille.message.HybridWorkerMessenger;
import com.kmwllc.lucille.message.LocalMessenger;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class WorkerIndexerTest {

  private final LinkedBlockingQueue<Document> pipelineDest = new LinkedBlockingQueue<>();
  private final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets = new LinkedBlockingQueue<>();

  @Test
  public void testDefaultMessengerIsHybridWorkerMessenger() throws Exception {
    try (MockedConstruction<HybridWorkerMessenger> construction = Mockito.mockConstruction(HybridWorkerMessenger.class,
        (mock, context) -> assertEquals(List.of(ConfigFactory.empty(), "pipeline1", pipelineDest, offsets),
            context.arguments()))) {
      WorkerMessenger messenger =
          WorkerIndexer.createWorkerMessenger(ConfigFactory.empty(), "pipeline1", pipelineDest, offsets);

      assertEquals(1, construction.constructed().size());
      assertSame(construction.constructed().get(0), messenger);
    }
  }

  @Test
  public void testConfiguredSubclassIsUsed() throws Exception {
    Config config = ConfigFactory.parseString(
        "kafka.events: false\nworker.messenger.class: \"" + SubclassedMessenger.class.getName() + "\"");

    WorkerMessenger messenger = WorkerIndexer.createWorkerMessenger(config, "pipeline1", pipelineDest, offsets);

    assertTrue(messenger instanceof SubclassedMessenger);
    SubclassedMessenger subclassed = (SubclassedMessenger) messenger;
    assertSame(config, subclassed.config);
    assertEquals("pipeline1", subclassed.pipelineName);
    assertSame(pipelineDest, subclassed.pipelineDest);
    assertSame(offsets, subclassed.offsets);

    // inherited behaviour still uses the queues it was given
    Document doc = Document.create("doc1");
    messenger.sendForIndexing(doc);
    assertSame(doc, pipelineDest.poll());
  }

  @Test
  public void testConfiguredMessengerNeedNotExtendHybridWorkerMessenger() throws Exception {
    Config config = ConfigFactory.parseString("worker.messenger.class: \"" + StandaloneMessenger.class.getName() + "\"");

    WorkerMessenger messenger = WorkerIndexer.createWorkerMessenger(config, "pipeline1", pipelineDest, offsets);

    assertTrue(messenger instanceof StandaloneMessenger);
  }

  @Test
  public void testInvalidMessengerClass() {
    Config missing = ConfigFactory.parseString("worker.messenger.class: \"com.kmwllc.lucille.DoesNotExist\"");
    assertThrows(IllegalArgumentException.class,
        () -> WorkerIndexer.createWorkerMessenger(missing, "pipeline1", pipelineDest, offsets));

    Config notAMessenger = ConfigFactory.parseString("worker.messenger.class: \"java.lang.String\"");
    assertThrows(IllegalArgumentException.class,
        () -> WorkerIndexer.createWorkerMessenger(notAMessenger, "pipeline1", pipelineDest, offsets));

    Config wrongConstructor = ConfigFactory.parseString("worker.messenger.class: \"" + LocalMessenger.class.getName() + "\"");
    assertThrows(IllegalArgumentException.class,
        () -> WorkerIndexer.createWorkerMessenger(wrongConstructor, "pipeline1", pipelineDest, offsets));
  }

  @Test
  public void testConstructorExceptionIsRethrown() {
    Config config = ConfigFactory.parseString("worker.messenger.class: \"" + FailingMessenger.class.getName() + "\"");

    IllegalStateException e = assertThrows(IllegalStateException.class,
        () -> WorkerIndexer.createWorkerMessenger(config, "pipeline1", pipelineDest, offsets));
    assertEquals("constructor failed", e.getMessage());
  }

  @Test
  public void testMessengerClassPassesConfigValidation() throws Exception {
    Config base = ConfigFactory.load("RunnerTest/singleDoc.conf");
    Config config = ConfigFactory.parseString("worker.messenger.class: \"" + SubclassedMessenger.class.getName() + "\"")
        .withFallback(base);

    assertFalse(Runner.runInValidationMode(config).containsKey("other"));

    Config invalid = ConfigFactory.parseString("worker.messenger.klass: \"" + SubclassedMessenger.class.getName() + "\"")
        .withFallback(base);

    assertTrue(Runner.runInValidationMode(invalid).containsKey("other"));
  }

  public static class SubclassedMessenger extends HybridWorkerMessenger {

    final Config config;
    final String pipelineName;
    final LinkedBlockingQueue<Document> pipelineDest;
    final LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets;

    public SubclassedMessenger(Config config, String pipelineName, LinkedBlockingQueue<Document> pipelineDest,
        LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
      super(config, pipelineName, pipelineDest, offsets, mock(KafkaConsumer.class));
      this.config = config;
      this.pipelineName = pipelineName;
      this.pipelineDest = pipelineDest;
      this.offsets = offsets;
    }
  }

  public static class StandaloneMessenger extends LocalMessenger {

    public StandaloneMessenger(Config config, String pipelineName, LinkedBlockingQueue<Document> pipelineDest,
        LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
      super();
    }
  }

  public static class FailingMessenger extends LocalMessenger {

    public FailingMessenger(Config config, String pipelineName, LinkedBlockingQueue<Document> pipelineDest,
        LinkedBlockingQueue<Map<TopicPartition, OffsetAndMetadata>> offsets) {
      throw new IllegalStateException("constructor failed");
    }
  }
}
