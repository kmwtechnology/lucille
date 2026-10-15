package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

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

  /**
   * Runs one document through a Worker whose pipeline is a single CreateChildrenStage with the given settings, using
   * the given mock messenger.
   */
  private static void runOneDocument(WorkerMessenger messenger, String stageSettings) throws Exception {
    Config config = ConfigFactory.parseString("pipelines: [{name: \"pipeline1\", stages: [{class: "
        + "\"com.kmwllc.lucille.stage.CreateChildrenStage\", " + stageSettings + "}]}]");
    AtomicReference<Worker> worker = new AtomicReference<>();
    when(messenger.pollDocToProcess()).thenReturn(Document.create("doc1", "run1")).thenAnswer(invocation -> {
      worker.get().terminate();
      return null;
    });
    worker.set(new Worker(config, messenger, "run1", "pipeline1", "WorkerTest"));
    worker.get().run();
  }

  private static Document withId(String id) {
    return argThat(d -> d != null && id.equals(d.getId()));
  }

  @Test
  public void testChildCreateEventsAreFlushedBeforeParentIsHandedOn() throws Exception {
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    runOneDocument(messenger, "numChildren: 2");

    // the parent's terminal event comes from the Indexer, possibly through another producer, so the children's
    // CREATE events must be delivered before the parent is sent for indexing
    InOrder order = inOrder(messenger);
    order.verify(messenger).sendEvent(withId("doc1_child1"), isNull(), eq(Event.Type.CREATE));
    order.verify(messenger).sendEvent(withId("doc1_child2"), isNull(), eq(Event.Type.CREATE));
    order.verify(messenger).flushEvents();
    order.verify(messenger).sendForIndexing(withId("doc1"));
    // and every event is delivered before the document's offset is committed
    order.verify(messenger).flushEvents();
    order.verify(messenger).commitPendingDocOffsets();
  }

  @Test
  public void testFailEventIsFlushedBeforeOffsetCommit() throws Exception {
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    runOneDocument(messenger, "numChildren: 2, failAfter: 1");

    InOrder order = inOrder(messenger);
    order.verify(messenger).sendEvent(withId("doc1_child1"), isNull(), eq(Event.Type.CREATE));
    order.verify(messenger).sendEvent(withId("doc1"), isNull(), eq(Event.Type.FAIL));
    order.verify(messenger).flushEvents();
    order.verify(messenger).commitPendingDocOffsets();
    verify(messenger, never()).sendForIndexing(withId("doc1"));
  }

  @Test
  public void testDroppedParentEventsAreFlushedBeforeOffsetCommit() throws Exception {
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    runOneDocument(messenger, "numChildren: 1, dropParent: true");

    InOrder order = inOrder(messenger);
    order.verify(messenger).sendEvent(withId("doc1_child1"), isNull(), eq(Event.Type.CREATE));
    order.verify(messenger).sendEvent(withId("doc1"), isNull(), eq(Event.Type.DROP));
    order.verify(messenger).flushEvents();
    order.verify(messenger).commitPendingDocOffsets();
  }

  @Test
  public void testFailedChildCreateEventFailsParent() throws Exception {
    // a CREATE event that could not be delivered must stop the parent from being handed on, as it did when each
    // event was sent synchronously: otherwise the parent could complete before the Publisher knows about the child
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    doThrow(new Exception("simulated event send failure")).doNothing().when(messenger).flushEvents();
    runOneDocument(messenger, "numChildren: 1");

    verify(messenger, never()).sendForIndexing(withId("doc1"));
    InOrder order = inOrder(messenger);
    order.verify(messenger).sendEvent(withId("doc1_child1"), isNull(), eq(Event.Type.CREATE));
    order.verify(messenger).flushEvents();
    order.verify(messenger).sendEvent(withId("doc1"), isNull(), eq(Event.Type.FAIL));
    order.verify(messenger).flushEvents();
    order.verify(messenger).commitPendingDocOffsets();
  }

  @Test
  public void testFailedFlushDoesNotBlockOffsetCommit() throws Exception {
    // as when sends were synchronous, an undeliverable event is logged and does not hold back the commit
    WorkerMessenger messenger = mock(WorkerMessenger.class);
    doThrow(new Exception("simulated event send failure")).when(messenger).flushEvents();
    runOneDocument(messenger, "numChildren: 1, dropParent: true");

    InOrder order = inOrder(messenger);
    order.verify(messenger).sendEvent(withId("doc1"), isNull(), eq(Event.Type.DROP));
    order.verify(messenger).flushEvents();
    order.verify(messenger).commitPendingDocOffsets();
  }
}
