package com.kmwllc.lucille.core;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class ZKRetryCounterTest {

  @Test
  public void testConstruct() {

  }

  @Test
  public void testAdd() {
    try (MockedConstruction<ExponentialBackoffRetry> policyConstruction = mockConstruction(ExponentialBackoffRetry.class);
         MockedStatic<CuratorFrameworkFactory> curatorStatic = mockStatic(CuratorFrameworkFactory.class)) {
      when(policyConstruction.equals(any())).thenReturn(true);
      CuratorFramework curatorFramework = mock(CuratorFramework.class);
      curatorStatic.when(() -> CuratorFrameworkFactory.newClient("foo", new ExponentialBackoffRetry(1000, 3))).thenReturn(curatorFramework);
      verify(curatorFramework, times(1)).start();
    }
  }
}
