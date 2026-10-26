package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class WorkerTest {

  @Test
  public void testMainArgs() throws Exception {
    Map<WorkerPool, List<Object>> args = new HashMap<>();
    Config config = ConfigFactory.load("WorkerTest/config.conf");
    WorkerMessengerFactory mockFactory = new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        throw new UnsupportedOperationException("Unimplemented method 'create'");
      }
    };
    try (MockedStatic<ConfigFactory> configFactory = Mockito.mockStatic(ConfigFactory.class);
        MockedConstruction<WorkerPool> workerPool = Mockito.mockConstruction(WorkerPool.class, (mock, context) -> {
          args.put(mock, new ArrayList<>(context.arguments()));
        });
        MockedStatic<WorkerMessengerFactory> factory = Mockito.mockStatic(WorkerMessengerFactory.class)) {

      configFactory.when(ConfigFactory::load).thenReturn(config);

      factory.when(() -> {
        WorkerMessengerFactory.getKafkaFactory(config, "foo");
      }).thenReturn(mockFactory);

      Worker.main(new String[] {"foo"});

      assertEquals(1, workerPool.constructed().size());
      WorkerPool pool = workerPool.constructed().get(0);

      assertEquals(config, args.get(pool).get(0));
      assertEquals("foo", args.get(pool).get(1));
      assertNull(args.get(pool).get(2));
      assertEquals(mockFactory, args.get(pool).get(3));
      assertEquals("foo", args.get(pool).get(4));
    }
  }
  @Test
  public void testMainNoArgs() throws Exception {
    Map<WorkerPool, List<Object>> args = new HashMap<>();
    Config config = ConfigFactory.load("WorkerTest/config.conf");
    WorkerMessengerFactory mockFactory2 = new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        throw new UnsupportedOperationException("Unimplemented method 'create'");
      }
    };
    try (MockedStatic<ConfigFactory> configFactory = Mockito.mockStatic(ConfigFactory.class);
        MockedConstruction<WorkerPool> workerPool = Mockito.mockConstruction(WorkerPool.class, (mock, context) -> {
          args.put(mock, new ArrayList<>(context.arguments()));
        });
        MockedStatic<WorkerMessengerFactory> factory = Mockito.mockStatic(WorkerMessengerFactory.class)) {

      configFactory.when(ConfigFactory::load).thenReturn(config);

      factory.when(() -> {
        WorkerMessengerFactory.getKafkaFactory(config, "foo2");
      }).thenReturn(mockFactory2);

      Worker.main(new String[0]);

      List<WorkerPool> constructed = workerPool.constructed();
      assertEquals(1, constructed.size());
      WorkerPool pool = constructed.get(0);

      assertEquals(config, args.get(pool).get(0));
      assertEquals("foo2", args.get(pool).get(1));
      assertNull(args.get(pool).get(2));
      assertEquals(mockFactory2, args.get(pool).get(3));
      assertEquals("foo2", args.get(pool).get(4));
    }
  }

  @Test
  public void testConfiguredRetryCounterIsUsed() throws Exception {
    Config config = ConfigFactory.load("WorkerTest/retryCounterClass.conf");
    Document doc = Document.create("doc1", "run1");
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    when(messenger.pollDocToProcess()).thenReturn(doc).thenThrow(new InterruptedException());

    try (MockedConstruction<ZKRetryCounter> zkCounter = Mockito.mockConstruction(ZKRetryCounter.class)) {
      Worker worker = new Worker(config, messenger, "run1", "pipeline1", "pipeline1");
      assertTrue(zkCounter.constructed().isEmpty());

      TestRetryCounter counter = TestRetryCounter.lastCreated;
      assertSame(config, counter.config);

      // TestRetryCounter reports every document as having exceeded the limit, so the worker sends it to the DLQ
      worker.run();

      assertEquals(List.of(doc), counter.added);
      assertEquals(List.of(doc), counter.removed);
      verify(messenger).sendFailed(doc);
      verify(messenger).sendEvent(doc, "SENT_TO_DLQ", Event.Type.FAIL);
      verify(messenger, never()).sendForIndexing(any());
    }
  }

  @Test
  public void testDefaultRetryCounterIsZK() throws Exception {
    Config config = ConfigFactory.load("WorkerTest/retryCounterDefault.conf");

    try (MockedConstruction<ZKRetryCounter> zkCounter = Mockito.mockConstruction(ZKRetryCounter.class, (mock, context) -> {
      assertEquals(List.of(config), context.arguments());
    })) {
      new Worker(config, mock(WorkerMessenger.class), "run1", "pipeline1", "pipeline1");
      assertEquals(1, zkCounter.constructed().size());

      // no retry counter is created when maxRetries is not set
      new Worker(config.withoutPath("worker.maxRetries"), mock(WorkerMessenger.class), "run1", "pipeline1", "pipeline1");
      assertEquals(1, zkCounter.constructed().size());
    }
  }

  @Test
  public void testInvalidRetryCounterClassFailsAtStartup() {
    Config config = ConfigFactory.load("WorkerTest/retryCounterClass.conf");

    Config unknown = config.withValue("worker.retryCounter.class", ConfigValueFactory.fromAnyRef("com.example.NoSuchCounter"));
    IllegalArgumentException e = assertThrows(IllegalArgumentException.class,
        () -> new Worker(unknown, mock(WorkerMessenger.class), "run1", "pipeline1", "pipeline1"));
    assertEquals("worker.retryCounter.class: could not find class com.example.NoSuchCounter", e.getMessage());

    Config notACounter = config.withValue("worker.retryCounter.class", ConfigValueFactory.fromAnyRef("java.lang.String"));
    e = assertThrows(IllegalArgumentException.class,
        () -> new Worker(notACounter, mock(WorkerMessenger.class), "run1", "pipeline1", "pipeline1"));
    assertEquals("worker.retryCounter.class: java.lang.String does not implement com.kmwllc.lucille.core.RetryCounter",
        e.getMessage());

    Config noConfigConstructor = config.withValue("worker.retryCounter.class",
        ConfigValueFactory.fromAnyRef(NoConfigConstructorRetryCounter.class.getName()));
    e = assertThrows(IllegalArgumentException.class,
        () -> new Worker(noConfigConstructor, mock(WorkerMessenger.class), "run1", "pipeline1", "pipeline1"));
    assertTrue(e.getMessage().endsWith("must have a public constructor that takes a Config"));
  }

  @Test
  public void testSpecAllowsRetryCounterClass() {
    Config config = ConfigFactory.load("WorkerTest/retryCounterClass.conf");
    Worker.SPEC.validate(config.getConfig("worker"), "worker");

    Config unknownProperty = config.withValue("worker.retryCounter.foo", ConfigValueFactory.fromAnyRef("bar"));
    assertThrows(IllegalArgumentException.class, () -> Worker.SPEC.validate(unknownProperty.getConfig("worker"), "worker"));
  }

  public static class TestRetryCounter implements RetryCounter {

    static TestRetryCounter lastCreated;

    final Config config;
    final List<Document> added = new ArrayList<>();
    final List<Document> removed = new ArrayList<>();

    public TestRetryCounter(Config config) {
      this.config = config;
      lastCreated = this;
    }

    @Override
    public boolean add(Document document) {
      added.add(document);
      return true;
    }

    @Override
    public void remove(Document document) {
      removed.add(document);
    }
  }

  public static class NoConfigConstructorRetryCounter implements RetryCounter {

    @Override
    public boolean add(Document document) {
      return false;
    }

    @Override
    public void remove(Document document) {
    }
  }
}
