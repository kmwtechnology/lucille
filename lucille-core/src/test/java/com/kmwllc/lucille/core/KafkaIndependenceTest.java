package com.kmwllc.lucille.core;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

/**
 * Verifies that the classes every run depends on can be loaded, and a local run completed, without kafka-clients on the
 * classpath. A catch of a Kafka exception is enough to break this, because the JVM's verifier resolves catch types when
 * it links a class.
 */
public class KafkaIndependenceTest {

  private static final String LOCAL_RUN_CONFIG = "connectors: [{name: \"c\", class: \"com.kmwllc.lucille.connector.SequenceConnector\", "
      + "numDocs: 3, pipeline: \"p\"}], pipelines: [{name: \"p\", stages: []}], indexer { type: \"Nop\" }";

  // Loads classes itself, rather than through the application class loader, so that their references come back to it.
  private static class NoKafkaClassLoader extends URLClassLoader {

    NoKafkaClassLoader(URL[] urls) {
      super(urls, ClassLoader.getPlatformClassLoader());
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
      if (name.startsWith("org.apache.kafka.")) {
        throw new ClassNotFoundException(name);
      }
      return super.loadClass(name, resolve);
    }
  }

  private static NoKafkaClassLoader noKafkaClassLoader() throws Exception {
    List<URL> urls = new ArrayList<>();
    for (String entry : System.getProperty("java.class.path").split(File.pathSeparator)) {
      urls.add(new File(entry).toURI().toURL());
    }
    return new NoKafkaClassLoader(urls.toArray(new URL[0]));
  }

  @Test
  public void testKafkaIsBlocked() throws Exception {
    try (NoKafkaClassLoader loader = noKafkaClassLoader()) {
      assertThrows(ClassNotFoundException.class,
          () -> Class.forName("org.apache.kafka.common.errors.RecordDeserializationException", false, loader));
    }
  }

  @Test
  public void testIndexerLoadsWithoutKafka() throws Exception {
    try (NoKafkaClassLoader loader = noKafkaClassLoader()) {
      Class.forName("com.kmwllc.lucille.core.Indexer", true, loader);
    }
  }

  @Test
  public void testWorkerLoadsWithoutKafka() throws Exception {
    try (NoKafkaClassLoader loader = noKafkaClassLoader()) {
      Class.forName("com.kmwllc.lucille.core.Worker", true, loader);
    }
  }

  // Validation runs first, which also covers ConfigValidator reading KafkaUtils.SPEC.
  @Test
  public void testLocalRunSucceedsWithoutKafka() throws Exception {
    ClassLoader original = Thread.currentThread().getContextClassLoader();
    try (NoKafkaClassLoader loader = noKafkaClassLoader()) {
      // IndexerFactory finds the registered indexers through the context class loader
      Thread.currentThread().setContextClassLoader(loader);

      Class<?> configClass = loader.loadClass("com.typesafe.config.Config");
      Object config = loader.loadClass("com.typesafe.config.ConfigFactory").getMethod("parseString", String.class)
          .invoke(null, LOCAL_RUN_CONFIG);
      Class<?> runType = loader.loadClass("com.kmwllc.lucille.core.Runner$RunType");
      Object result = loader.loadClass("com.kmwllc.lucille.core.Runner").getMethod("run", configClass, runType)
          .invoke(null, config, runType.getField("LOCAL").get(null));

      assertTrue((Boolean) result.getClass().getMethod("getStatus").invoke(result));
    } finally {
      Thread.currentThread().setContextClassLoader(original);
    }
  }
}
