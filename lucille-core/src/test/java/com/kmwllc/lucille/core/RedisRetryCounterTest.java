package com.kmwllc.lucille.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.ConfigFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class RedisRetryCounterTest {

  /**
   * A JedisPool backed by an in-memory map so tests never need a live Redis server
   * and avoid Mockito/Byte Buddy issues with the large Jedis class hierarchy.
   */
  private static class FakeJedisPool extends JedisPool {

    final Map<String, Long> counters = new HashMap<>();
    final List<String> deleted = new ArrayList<>();
    final List<long[]> expiries = new ArrayList<>();
    boolean throwOnNext = false;

    FakeJedisPool() {
      super(new GenericObjectPoolConfig<>(), "localhost", 6379, 1, null, 0, null);
    }

    @Override
    public Jedis getResource() {
      if (throwOnNext) {
        throw new RuntimeException("Redis unavailable");
      }
      return new Jedis("localhost", 6379) {
        @Override
        public long incr(String key) {
          long val = counters.getOrDefault(key, 0L) + 1;
          counters.put(key, val);
          return val;
        }

        @Override
        public long expire(String key, long seconds) {
          expiries.add(new long[]{seconds});
          return 1L;
        }

        @Override
        public long del(String... keys) {
          for (String k : keys) {
            counters.remove(k);
            deleted.add(k);
          }
          return (long) keys.length;
        }

        @Override
        public long del(String key) {
          counters.remove(key);
          deleted.add(key);
          return 1L;
        }

        @Override
        public void close() {
          // no-op
        }
      };
    }
  }

  /** Creates a KafkaDocument with a known runId without needing Mockito spy. */
  private static KafkaDocument kafkaDoc(String id, String runId, String topic, int partition, long offset, String key)
      throws DocumentException {
    return new KafkaDocument(Document.create(id, runId), topic, partition, offset, key);
  }

  @Test
  public void testAddExceedsMax() throws Exception {
    FakeJedisPool pool = new FakeJedisPool();
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), pool);

    KafkaDocument doc = kafkaDoc("kafkaDoc", "runId", "topic", 1, 2, "key");

    // maxRetries defaults to 3; call add 4 times — 4th should return true
    assertFalse(counter.add(doc));
    assertFalse(counter.add(doc));
    assertFalse(counter.add(doc));
    assertTrue(counter.add(doc));
  }

  @Test
  public void testAddWithinMax() throws Exception {
    FakeJedisPool pool = new FakeJedisPool();
    // retry10.conf sets maxRetries: 10
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/retry10.conf"), pool);

    Document doc = Document.create("jsonDoc", "runId2");
    for (int i = 0; i < 5; i++) {
      assertFalse(counter.add(doc));
    }
  }

  @Test
  public void testAddSetsExpiry() throws Exception {
    FakeJedisPool pool = new FakeJedisPool();
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), pool);

    Document doc = Document.create("jsonDoc", "runId1");
    counter.add(doc);

    assertEquals(1, pool.expiries.size());
    assertEquals(RedisRetryCounter.DEFAULT_TTL_SECONDS, pool.expiries.get(0)[0]);
  }

  @Test
  public void testRemoveKafkaAndNonKafka() throws Exception {
    FakeJedisPool pool = new FakeJedisPool();
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), pool);

    KafkaDocument kafkaDoc = kafkaDoc("kafkaDoc", "runId", "topic", 1, 2, "key");
    Document doc = Document.create("jsonDoc", "runId2");

    // seed counters first
    counter.add(kafkaDoc);
    counter.add(doc);

    counter.remove(kafkaDoc);
    counter.remove(doc);

    assertEquals(2, pool.deleted.size());
    assertTrue(pool.deleted.get(0).contains("topic"));
    assertTrue(pool.deleted.get(1).contains("NON_KAFKA"));
  }

  @Test
  public void testAddThrowsReturnsOptimisticFalse() throws Exception {
    FakeJedisPool pool = new FakeJedisPool();
    pool.throwOnNext = true;

    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), pool);

    Document doc = Document.create("jsonDoc", "runId1");
    // on error, optimistically returns false (not exceeded)
    assertFalse(counter.add(doc));
  }

  @Test
  public void testKeyFormatKafkaDocument() throws Exception {
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), new FakeJedisPool());

    KafkaDocument doc = kafkaDoc("kafkaDoc", "run1", "myTopic", 3, 7, "myKey");
    // expected: LucilleCounters:bar:myTopic:run1:myKey___3_7
    assertEquals("LucilleCounters:bar:myTopic:run1:myKey___3_7", counter.getKey(doc));
  }

  @Test
  public void testKeyFormatNonKafkaDocument() throws Exception {
    RedisRetryCounter counter = new RedisRetryCounter(
        ConfigFactory.load("RedisRetryCounterTest/config.conf"), new FakeJedisPool());

    Document doc = Document.create("myDoc", "run2");
    // expected: LucilleCounters:bar:NON_KAFKA:run2:myDoc
    assertEquals("LucilleCounters:bar:NON_KAFKA:run2:myDoc", counter.getKey(doc));
  }
}
