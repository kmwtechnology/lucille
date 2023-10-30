package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZKRetryCounter implements RetryCounter {

  private static final Logger log = LoggerFactory.getLogger(ZKRetryCounter.class);
  private final CuratorFramework curatorFramework;
  private final int maxRetries;

  private final String retryCounterPrefix;

  public ZKRetryCounter(Config config) {
    this.maxRetries = config.hasPath("worker.maxRetries") ? config.getInt("worker.maxRetries") : 3;
    this.retryCounterPrefix = "/LucilleCounters/" + config.getString("kafka.consumerGroupId") + "/";

    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    this.curatorFramework = CuratorFrameworkFactory.newClient(config.getString("zookeeper.connectString"), retryPolicy);
    curatorFramework.start();
  }

  @Override
  public boolean add(Document document) {
    String counterPath = getCounterPath(document);
    int retryCount = 0;
    try (SharedCount counter = new SharedCount(curatorFramework, counterPath, 0)) {
      counter.start();
      retryCount = counter.getCount();
      counter.setCount(++retryCount);
    } catch (Exception e) {
      log.error("Couldn't access retry counter for doc " + document.getId(), e);
    }
    // if we weren't able to access the retry counter we optimistically assume the document
    // has not exceeded the max
    return retryCount > maxRetries;
  }

  @Override
  public void remove(Document document) {
    String counterPath = getCounterPath(document);
    try {
      curatorFramework.delete().quietly().deletingChildrenIfNeeded().forPath(counterPath);
    } catch (Exception e) {
      log.error("Couldn't delete retry counter for doc " + document.getId(), e);
    }
  }

  private String getCounterPath(Document document) {
    if (document instanceof KafkaDocument) {
      KafkaDocument doc = (KafkaDocument) document;
      return retryCounterPrefix + doc.getTopic() + "/" + doc.getRunId() + "/" + doc.getKey() + "___" + doc.getPartition() + "_"
          + doc.getOffset();
    }
    return retryCounterPrefix + "/NON_KAFKA/" + document.getRunId() + "/" + document.getId();
  }
}
