package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.message.WorkerMessenger;
import com.kmwllc.lucille.message.WorkerMessengerFactory;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class LaunchTest {

  @Before
  public void reset() {
    Launch.resetShutdownState();
  }

  @After
  public void cleanUp() {
    Launch.resetShutdownState();
  }

  @Test
  public void testLowerCaseOptionName() {
    // option names are case insensitive, but anything that is not an option name is left alone
    assertEquals("-validate", Launch.lowerCaseOptionName("-Validate"));
    assertEquals("--RENDER".toLowerCase(), Launch.lowerCaseOptionName("--RENDER"));
    assertEquals("MyPipeline", Launch.lowerCaseOptionName("MyPipeline"));
    assertEquals("", Launch.lowerCaseOptionName(""));
  }

  @Test
  public void testShutdownActionsRunInOrderAndAFailureDoesNotStopTheRest() {
    List<String> ran = new ArrayList<>();
    Launch.installShutdownHook(() -> ran.add("first"));
    Launch.installShutdownHook(() -> {
      throw new RuntimeException("second fails");
    });
    Launch.installShutdownHook(() -> ran.add("third"));

    assertFalse(Launch.isShuttingDown());
    Launch.runShutdownActions();

    assertEquals(List.of("first", "third"), ran);
    assertTrue(Launch.isShuttingDown());
  }

  @Test
  public void testExitUnlessShuttingDown() {
    try (MockedStatic<Launch.SystemHelper> helper = Mockito.mockStatic(Launch.SystemHelper.class)) {
      // not shutting down: the process exits with the given code
      Launch.exitUnlessShuttingDown(3);
      helper.verify(() -> Launch.SystemHelper.exit(3), times(1));

      // shutting down after a signal: the JVM's own exit status must stand, so no exit() call
      Launch.runShutdownActions();
      Launch.exitUnlessShuttingDown(1);
      helper.verify(() -> Launch.SystemHelper.exit(1), never());
      helper.verify(() -> Launch.SystemHelper.exit(anyInt()), times(1));
    }
  }

  @Test
  public void testWorkerExitsWithOneWhenAWorkerFails() throws Exception {
    Config config = ConfigFactory.load("WorkerTest/config.conf");
    WorkerMessengerFactory messengerFactory = new WorkerMessengerFactory() {
      @Override
      public WorkerMessenger create() {
        throw new UnsupportedOperationException("not needed");
      }
    };
    List<Integer> exitCodes = new ArrayList<>();

    try (MockedStatic<Launch.SystemHelper> helper = Mockito.mockStatic(Launch.SystemHelper.class);
        MockedStatic<ConfigFactory> configFactory = Mockito.mockStatic(ConfigFactory.class);
        MockedStatic<WorkerMessengerFactory> factory = Mockito.mockStatic(WorkerMessengerFactory.class);
        MockedConstruction<WorkerPool> pools = Mockito.mockConstruction(WorkerPool.class, (mock, context) -> {
          // the pool is alive but one of its workers has died
          when(mock.isAlive()).thenReturn(true);
          when(mock.isFailed()).thenReturn(true);
          when(mock.getThrowable()).thenReturn(new RuntimeException("worker died"));
        })) {
      helper.when(() -> Launch.SystemHelper.exit(anyInt())).thenAnswer(invocation -> {
        exitCodes.add(invocation.getArgument(0));
        return null;
      });
      configFactory.when(ConfigFactory::load).thenReturn(config);
      factory.when(() -> WorkerMessengerFactory.getKafkaFactory(config, "foo")).thenReturn(messengerFactory);

      Launch.worker(new String[] {"foo"});

      assertEquals(1, pools.constructed().size());
      WorkerPool pool = pools.constructed().get(0);
      verify(pool).start();
      // the remaining workers are stopped and waited for before the process exits
      verify(pool).stop();
      verify(pool).join(anyLong());
      assertEquals(List.of(1), exitCodes);
    }
  }
}
