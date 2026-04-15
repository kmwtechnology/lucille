package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * A {@link RetryCounter} implementation that uses Redis (via Jedis) to store per-document retry
 * counts. Counters are stored as Redis keys with an optional TTL so they are automatically cleaned
 * up even if {@link #remove(Document)} is never called.
 *
 * <p>Required configuration:
 * <ul>
 *   <li>{@code redis.host} – Redis host (default: {@code localhost})</li>
 *   <li>{@code redis.port} – Redis port (default: {@code 6379})</li>
 * </ul>
 *
 * <p>Optional configuration:
 * <ul>
 *   <li>{@code redis.ttlSeconds} – TTL in seconds for each counter key (default: {@code 86400})</li>
 * </ul>
 */
public class RedisRetryCounter implements RetryCounter {

  private static final Logger log = LoggerFactory.getLogger(RedisRetryCounter.class);

  static final int DEFAULT_TTL_SECONDS = 86400;
  static final String KEY_PREFIX = "LucilleCounters:";

  // Functional interface for the Redis operations we need, enabling easy unit testing.
  @FunctionalInterface
  interface RedisOperation<T> {
    T execute(Jedis jedis);
  }

  private final JedisPool jedisPool;
  private final int maxRetries;
  private final long ttlSeconds;
  private final String keyPrefix;

  public RedisRetryCounter(Config config) {
    this.maxRetries = config.hasPath("worker.maxRetries") ? config.getInt("worker.maxRetries") : 3;
    this.ttlSeconds = config.hasPath("redis.ttlSeconds") ? config.getLong("redis.ttlSeconds") : DEFAULT_TTL_SECONDS;
    String consumerGroupId = config.hasPath("kafka.consumerGroupId") ? config.getString("kafka.consumerGroupId") : "default";
    this.keyPrefix = KEY_PREFIX + consumerGroupId + ":";

    String host = config.hasPath("redis.host") ? config.getString("redis.host") : "localhost";
    int port = config.hasPath("redis.port") ? config.getInt("redis.port") : 6379;
    this.jedisPool = new JedisPool(new JedisPoolConfig(), host, port);
  }

  // Package-private constructor for testing with a pre-built pool
  RedisRetryCounter(Config config, JedisPool jedisPool) {
    this.maxRetries = config.hasPath("worker.maxRetries") ? config.getInt("worker.maxRetries") : 3;
    this.ttlSeconds = config.hasPath("redis.ttlSeconds") ? config.getLong("redis.ttlSeconds") : DEFAULT_TTL_SECONDS;
    String consumerGroupId = config.hasPath("kafka.consumerGroupId") ? config.getString("kafka.consumerGroupId") : "default";
    this.keyPrefix = KEY_PREFIX + consumerGroupId + ":";
    this.jedisPool = jedisPool;
  }

  @Override
  public boolean add(Document document) {
    String key = getKey(document);
    long retryCount = 0;
    try (Jedis jedis = jedisPool.getResource()) {
      retryCount = jedis.incr(key);
      jedis.expire(key, ttlSeconds);
    } catch (Exception e) {
      log.error("Couldn't access Redis retry counter for doc " + document.getId(), e);
      // optimistically assume the document has not exceeded the max
      return false;
    }
    return retryCount > maxRetries;
  }

  @Override
  public void remove(Document document) {
    String key = getKey(document);
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(key);
    } catch (Exception e) {
      log.error("Couldn't delete Redis retry counter for doc " + document.getId(), e);
    }
  }

  // Package-private for testing
  String getKey(Document document) {
    if (document instanceof KafkaDocument) {
      KafkaDocument doc = (KafkaDocument) document;
      return keyPrefix + doc.getTopic() + ":" + doc.getRunId() + ":" + doc.getKey()
          + "___" + doc.getPartition() + "_" + doc.getOffset();
    }
    return keyPrefix + "NON_KAFKA:" + document.getRunId() + ":" + document.getId();
  }
}
