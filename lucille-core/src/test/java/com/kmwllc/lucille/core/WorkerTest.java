package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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

      assertEquals(args.get(pool).get(0), config);
      assertEquals(args.get(pool).get(1), "foo");
      assertEquals(args.get(pool).get(2), mockFactory);
      assertEquals(args.get(pool).get(3), "foo");
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

      assertEquals(args.get(pool).get(0), config);
      assertEquals(args.get(pool).get(1), "foo2");
      assertEquals(args.get(pool).get(2), mockFactory2);
      assertEquals(args.get(pool).get(3), "foo2");
    }
  }
}
