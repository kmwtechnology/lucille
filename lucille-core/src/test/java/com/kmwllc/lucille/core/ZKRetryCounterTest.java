package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundVersionable;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.DeleteBuilderMain;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import com.typesafe.config.ConfigFactory;

public class ZKRetryCounterTest {

  @Test
  public void testRemove() throws Exception {
    String jsonCounterString = "/LucilleCounters/bar//NON_KAFKA/runId2/jsonDoc";
    String kafkaCounterString = "/LucilleCounters/bar/topic/runId/key___1_2";
    try (MockedStatic<CuratorFrameworkFactory> curatorStatic = mockStatic(CuratorFrameworkFactory.class)) {
      BackgroundVersionable backgroundVersionable = mock(BackgroundVersionable.class);
      doThrow(Exception.class).when(backgroundVersionable).forPath(kafkaCounterString);
      DeleteBuilderMain deleteBuilderMain = mock(DeleteBuilderMain.class);
      when(deleteBuilderMain.deletingChildrenIfNeeded()).thenReturn(backgroundVersionable);
      DeleteBuilder deleteBuilder = mock(DeleteBuilder.class);
      when(deleteBuilder.quietly()).thenReturn(deleteBuilderMain);
      CuratorFramework curatorFramework = mock(CuratorFramework.class);
      when(curatorFramework.delete()).thenReturn(deleteBuilder);
      curatorStatic.when(() -> CuratorFrameworkFactory.newClient(eq("foo"), any())).thenReturn(curatorFramework);

      KafkaDocument kafkaDoc = spy(new KafkaDocument(Document.create("kafkaDoc"), "topic", 1, 2, "key"));
      when(kafkaDoc.getRunId()).thenReturn("runId");
      JsonDocument jsonDoc = new JsonDocument("jsonDoc", "runId2");

      ZKRetryCounter retry3 = new ZKRetryCounter(ConfigFactory.load("ZKRetryCounterTest/config.conf"));
      ArgumentCaptor<String> captor = ArgumentCaptor.forClass(String.class);

      retry3.remove(jsonDoc);
      retry3.remove(kafkaDoc);

      verify(backgroundVersionable, times(2)).forPath(captor.capture());

      assertEquals(jsonCounterString, captor.getAllValues().get(0));
      assertEquals(kafkaCounterString, captor.getAllValues().get(1));
    }
  }

  @Test 
  public void testConstructor() {
    try (MockedStatic<CuratorFrameworkFactory> curatorStatic = mockStatic(CuratorFrameworkFactory.class)) {
      CuratorFramework curatorFramework = mock(CuratorFramework.class);
      curatorStatic.when(() -> CuratorFrameworkFactory.newClient(eq("foo"), any())).thenReturn(curatorFramework);
      new ZKRetryCounter(ConfigFactory.load("ZKRetryCounterTest/config.conf"));
      verify(curatorFramework, times(1)).start();
    }
  }

  @Test
  public void testAdd() throws Exception {
    Map<SharedCount, List<Object>> arguments = new HashMap<>();
    try (MockedStatic<CuratorFrameworkFactory> curatorStatic = mockStatic(CuratorFrameworkFactory.class);
         MockedConstruction<SharedCount> sharedCount = mockConstruction(SharedCount.class, (mock, context) -> {
          arguments.put(mock, (List<Object>)context.arguments());
          when(mock.getCount()).thenReturn(5);
    })) {
      CuratorFramework curatorFramework = mock(CuratorFramework.class);
      curatorStatic.when(() -> CuratorFrameworkFactory.newClient(eq("foo"), any())).thenReturn(curatorFramework);

      KafkaDocument kafkaDoc = spy(new KafkaDocument(Document.create("kafkaDoc"), "topic", 1, 2, "key"));
      when(kafkaDoc.getRunId()).thenReturn("runId");
      JsonDocument jsonDoc = new JsonDocument("jsonDoc", "runId2");

      ZKRetryCounter retry3 = new ZKRetryCounter(ConfigFactory.load("ZKRetryCounterTest/config.conf"));
      ZKRetryCounter retry10 = new ZKRetryCounter(ConfigFactory.load("ZKRetryCounterTest/retry10.conf"));

      assertTrue(retry3.add(kafkaDoc));
      assertTrue(retry3.add(jsonDoc));
      assertFalse(retry10.add(jsonDoc));

      assertEquals(3, sharedCount.constructed().size());

      SharedCount constructedFirst = sharedCount.constructed().get(0);
      SharedCount constructedSecond = sharedCount.constructed().get(1);
      SharedCount constructedThird = sharedCount.constructed().get(2);

      verify(constructedFirst, times(1)).start();
      verify(constructedSecond, times(1)).start();
      verify(constructedThird, times(1)).start();
      
      verify(constructedFirst, times(1)).setCount(6);
      verify(constructedSecond, times(1)).setCount(6);
      verify(constructedThird, times(1)).setCount(6);

      assertEquals(curatorFramework, arguments.get(constructedFirst).get(0));
      assertEquals(curatorFramework, arguments.get(constructedSecond).get(0));
      assertEquals(curatorFramework, arguments.get(constructedThird).get(0));

      assertEquals("/LucilleCounters/bar/topic/runId/key___1_2", arguments.get(constructedFirst).get(1));
      assertEquals("/LucilleCounters/bar//NON_KAFKA/runId2/jsonDoc", arguments.get(constructedSecond).get(1));
      assertEquals("/LucilleCounters/bar//NON_KAFKA/runId2/jsonDoc", arguments.get(constructedThird).get(1));

      assertEquals(0, arguments.get(constructedFirst).get(2));
      assertEquals(0, arguments.get(constructedSecond).get(2));
      assertEquals(0, arguments.get(constructedThird).get(2));
    }
  }

  @Test
  public void testAddThrows() throws Exception {
    Map<SharedCount, List<Object>> arguments = new HashMap<>();
    try (MockedStatic<CuratorFrameworkFactory> curatorStatic = mockStatic(CuratorFrameworkFactory.class);
         MockedConstruction<SharedCount> sharedCount = mockConstruction(SharedCount.class, (mock, context) -> {
          arguments.put(mock, (List<Object>)context.arguments());
          when(mock.getCount()).thenReturn(5);
          doThrow(Exception.class).when(mock).start();
    })) {
      CuratorFramework curatorFramework = mock(CuratorFramework.class);
      curatorStatic.when(() -> CuratorFrameworkFactory.newClient(eq("foo"), any())).thenReturn(curatorFramework);

      KafkaDocument kafkaDoc = spy(new KafkaDocument(Document.create("kafkaDoc"), "topic", 1, 2, "key"));
      when(kafkaDoc.getRunId()).thenReturn("runId");

      ZKRetryCounter retry3 = new ZKRetryCounter(ConfigFactory.load("ZKRetryCounterTest/config.conf"));

      assertFalse(retry3.add(kafkaDoc));
    }
  }
}
