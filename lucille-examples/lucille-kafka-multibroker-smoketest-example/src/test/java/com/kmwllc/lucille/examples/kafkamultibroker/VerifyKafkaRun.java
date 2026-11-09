package com.kmwllc.lucille.examples.kafkamultibroker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.Test;

/**
 * Checks a finished run against the three-broker cluster. Run it while the cluster is still up.
 *
 * <p>The output must contain every input id, and no others. Duplicates are allowed: delivery is at-least-once, so a
 * consumer that has to restart after a broker loss can re-send documents it had not yet committed. The event topic is
 * the one Lucille created for the run; it must be replicated three ways and require two in-sync replicas.
 *
 * <p>Environment: INPUT_CSV (the input the run read), OUTPUT_CSV (the file the indexer wrote), and KAFKA_BOOTSTRAP_SERVERS
 * (the brokers' EXTERNAL listeners).
 */
public class VerifyKafkaRun {

  private static final String BOOTSTRAP = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS",
      "localhost:19092,localhost:19093,localhost:19094");

  @Test
  public void testEveryInputDocumentWasIndexed() throws IOException {
    Set<String> expected = new HashSet<>(idsInOrdered(Paths.get(System.getenv("INPUT_CSV")), true));
    List<String> written = idsInOrdered(Paths.get(System.getenv("OUTPUT_CSV")), false);
    Set<String> actual = new HashSet<>(written);

    Set<String> missing = new HashSet<>(expected);
    missing.removeAll(actual);
    Set<String> unexpected = new HashSet<>(actual);
    unexpected.removeAll(expected);

    assertTrue("documents missing from the output: " + missing.size() + " e.g. " + sample(missing), missing.isEmpty());
    assertTrue("unexpected documents in the output: " + unexpected.size(), unexpected.isEmpty());
    System.out.println("indexed rows: " + written.size() + ", distinct ids: " + actual.size()
        + ", duplicate rows: " + (written.size() - actual.size()));
  }

  @Test
  public void testEventTopicIsReplicatedWithMinInsyncReplicas() throws Exception {
    Properties props = new Properties();
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);

    try (Admin admin = Admin.create(props)) {
      // Lucille names the event topic pipeline1_event_<runId>, and each scenario runs on a fresh cluster, so there is one.
      List<String> eventTopics = admin.listTopics().names().get().stream()
          .filter(name -> name.startsWith("pipeline1_event_")).collect(Collectors.toList());
      assertEquals("event topics: " + eventTopics, 1, eventTopics.size());

      TopicDescription description = admin.describeTopics(eventTopics).allTopicNames().get().get(eventTopics.get(0));
      for (TopicPartitionInfo partition : description.partitions()) {
        assertEquals("replicas of " + partition, 3, partition.replicas().size());
      }

      Map<ConfigResource, Config> configs = admin.describeConfigs(List.of(
          new ConfigResource(ConfigResource.Type.TOPIC, eventTopics.get(0)))).all().get();
      String minIsr = configs.values().iterator().next().get(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG).value();
      assertEquals("2", minIsr);
    }
  }

  // The first column of each line, in file order. The input CSV has a header row; the indexer's output does not.
  private static List<String> idsInOrdered(Path csv, boolean hasHeader) throws IOException {
    List<String> lines = Files.readAllLines(csv);
    return lines.stream().skip(hasHeader ? 1 : 0).filter(line -> !line.isBlank())
        .map(VerifyKafkaRun::firstColumn).collect(Collectors.toList());
  }

  // The id is the first column. The indexer quotes every value, so the quotes are removed.
  private static String firstColumn(String line) {
    String first = line.substring(0, line.indexOf(','));
    return first.length() >= 2 && first.startsWith("\"") && first.endsWith("\"")
        ? first.substring(1, first.length() - 1) : first;
  }

  private static String sample(Collection<String> ids) {
    return ids.stream().limit(5).collect(Collectors.joining(", "));
  }
}
