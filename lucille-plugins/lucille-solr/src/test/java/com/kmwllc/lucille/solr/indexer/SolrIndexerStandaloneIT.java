package com.kmwllc.lucille.solr.indexer;

import static org.mockito.Mockito.mock;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerFailure;
import com.kmwllc.lucille.core.IndexerRetryableException;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.IndexerMessenger;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.solr.connector.SolrConnector;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.SolrQuery;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Runs the SolrIndexer and SolrConnector against a standalone (non-cloud) Solr, configured as a user would: solr.url
 * without useCloudClient. SolrIndexerIT covers SolrCloud.
 */
@SuppressSSL
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public class SolrIndexerStandaloneIT extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  private static final String CORE = "standalone";

  @BeforeClass
  public static void startSolr() throws Exception {
    Path solrHome = createTempDir("solrhome");
    // without one, Solr looks for its default solr.xml under tests.src.home, which the pom points at test-classes
    Files.writeString(solrHome.resolve("solr.xml"), "<solr/>");

    Path configSet = Path.of(SolrIndexerStandaloneIT.class.getClassLoader()
        .getResource("SolrIndexerIntegrationTest/configsets/test/conf").toURI());
    Path target = solrHome.resolve("configsets/test/conf");
    try (Stream<Path> files = Files.walk(configSet)) {
      for (Path source : (Iterable<Path>) files::iterator) {
        Path dest = target.resolve(configSet.relativize(source).toString());
        if (Files.isDirectory(source)) {
          Files.createDirectories(dest);
        } else {
          Files.copy(source, dest);
        }
      }
    }

    solrRule.startSolr(solrHome);
    solrRule.newCollection(CORE).withConfigSet("test").create();
  }

  @Before
  public void clearCore() throws Exception {
    solrClient().deleteByQuery("*:*");
    solrClient().commit();
  }

  private static SolrClient solrClient() {
    return solrRule.getSolrClient(CORE);
  }

  private static Map<String, Object> baseConfig(String solrBlock) {
    Map<String, Object> map = new HashMap<>();
    // the documented single-node configuration: a url, no useCloudClient
    map.put(solrBlock + ".url", List.of(solrRule.getBaseUrl()));
    map.put(solrBlock + ".defaultCollection", CORE);
    return map;
  }

  private static SolrIndexer newIndexer(Map<String, Object> extra) {
    Map<String, Object> map = baseConfig("indexer.solr");
    map.put("indexer.type", "solr");
    map.put("indexer.batchSize", 100);
    map.putAll(extra);
    return new SolrIndexer(ConfigFactory.parseMap(map).getConfig("indexer"), new IndexerContext(mock(IndexerMessenger.class), false, "solr", null));
  }

  private static List<String> idsInSolr() throws Exception {
    solrClient().commit();
    SolrQuery q = new SolrQuery("*:*");
    q.setRows(100);
    return solrClient().query(q).getResults().stream()
        .map(d -> (String) d.getFieldValue("id"))
        .sorted()
        .collect(Collectors.toList());
  }

  @Test
  public void testIndexWithStandaloneClient() throws Exception {
    SolrIndexer indexer = newIndexer(Map.of());
    try {
      assertTrue(indexer.validateConnection());

      Document doc1 = Document.create("doc1");
      doc1.setField("name_s", "one");
      Document doc2 = Document.create("doc2");
      doc2.setField("name_s", "two");
      assertEquals(List.of(), indexer.sendToIndex(List.of(doc1, doc2)));

      assertEquals(List.of("doc1", "doc2"), idsInSolr());
    } finally {
      indexer.closeConnection();
    }
  }

  @Test
  public void testDeleteByFieldWithCommaQuoteBraceAndBackslash() throws Exception {
    List<String> special = List.of("Smith, John", "it's \"quoted\"", "{!curly}", "back\\slash", "a|b;c^d~e\tf,g");
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < special.size(); i++) {
      docs.add(new SolrInputDocument("id", "special" + i, "author_s", special.get(i)));
    }
    // these match the pieces of "Smith, John" and must survive its delete
    docs.add(new SolrInputDocument("id", "smith", "author_s", "Smith"));
    docs.add(new SolrInputDocument("id", "john", "author_s", " John"));
    docs.add(new SolrInputDocument("id", "byId", "author_s", "someone"));
    docs.add(new SolrInputDocument("id", "keep", "author_s", "keep"));
    solrClient().add(docs);
    solrClient().commit();

    SolrIndexer indexer = newIndexer(Map.of(
        "indexer.deletionMarkerField", "is_delete",
        "indexer.deletionMarkerFieldValue", "true",
        "indexer.deleteByFieldField", "delete_field",
        "indexer.deleteByFieldValue", "delete_value"));
    try {
      List<Document> deletes = new ArrayList<>();
      for (int i = 0; i < special.size(); i++) {
        Document delete = Document.create("delete" + i);
        delete.setField("is_delete", "true");
        delete.setField("delete_field", "author_s");
        delete.setField("delete_value", special.get(i));
        deletes.add(delete);
      }
      // a delete by id in the same batch is sent in the same query, on the id field
      Document deleteById = Document.create("byId");
      deleteById.setField("is_delete", "true");
      deletes.add(deleteById);

      assertEquals(List.of(), indexer.sendToIndex(deletes));
      assertEquals(List.of("john", "keep", "smith"), idsInSolr());
    } finally {
      indexer.closeConnection();
    }
  }

  @Test
  public void testIdIsSentWhenFieldFilterRemovesIt() throws Exception {
    SolrIndexer whitelist = newIndexer(Map.of("indexer.whitelist", List.of("name_s")));
    SolrIndexer blacklistWithOverride = newIndexer(Map.of(
        "indexer.blacklist", List.of("id"),
        "indexer.idOverrideField", "other_id"));
    try {
      Document doc1 = Document.create("doc1");
      doc1.setField("name_s", "one");
      doc1.setField("other_s", "dropped");
      assertEquals(List.of(), whitelist.sendToIndex(List.of(doc1)));

      Document doc2 = Document.create("doc2");
      doc2.setField("other_id", "overridden");
      assertEquals(List.of(), blacklistWithOverride.sendToIndex(List.of(doc2)));

      assertEquals(List.of("doc1", "overridden"), idsInSolr());
    } finally {
      whitelist.closeConnection();
      blacklistWithOverride.closeConnection();
    }
  }

  @Test
  public void testRealSolrErrorReportedWhenFieldFilterRemovesId() throws Exception {
    SolrIndexer indexer = newIndexer(Map.of("indexer.whitelist", List.of("count_i")));
    try {
      Document bad = Document.create("bad");
      bad.setField("count_i", "not a number");

      List<IndexerFailure> failed = indexer.sendToIndex(List.of(bad));

      assertEquals(1, failed.size());
      IndexerFailure failure = failed.get(0);
      assertSame(bad, failure.document());
      // Solr's own rejection of the value, not a NullPointerException from the failure accounting
      assertTrue(failure.cause() instanceof IndexerRetryableException);
      assertEquals(400, ((IndexerRetryableException) failure.cause()).getStatusCode());
    } finally {
      indexer.closeConnection();
    }
  }

  @Test
  public void testConnectorQueriesTypedAndMixedCaseFields() throws Exception {
    Date created = Date.from(Instant.parse("2026-10-29T12:00:00Z"));
    List<SolrInputDocument> docs = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      SolrInputDocument doc = new SolrInputDocument("id", "doc" + i);
      doc.setField("devId_i", i);
      doc.setField("price_d", i + 0.5);
      doc.setField("created_dt", created);
      doc.setField("counts_is", List.of(i, i + 10));
      doc.setField("tags_ss", List.of("a" + i, "b" + i));
      docs.add(doc);
    }
    solrClient().add(docs);
    solrClient().commit();

    Map<String, Object> map = baseConfig("solr");
    map.put("name", "connector1");
    map.put("pipeline", "pipeline1");
    // fewer rows than documents, so the connector has to follow the cursor across pages
    map.put("solrParams", Map.of("q", "*:*", "rows", 2));
    Config config = ConfigFactory.parseMap(map);

    TestMessenger messenger = new TestMessenger();
    SolrConnector connector = new SolrConnector(config);
    try {
      connector.execute(new PublisherImpl(config, messenger, "run", "pipeline1"));
    } finally {
      connector.close();
    }

    List<Document> published = messenger.getDocsSentForProcessing();
    assertEquals(List.of("doc0", "doc1", "doc2", "doc3", "doc4"),
        published.stream().map(Document::getId).collect(Collectors.toList()));

    Document doc3 = published.get(3);
    // the field keeps its name and its Solr type (compared as JSON nodes, since the typed getters would also accept
    // a string)
    assertEquals(IntNode.valueOf(3), doc3.getJson("devId_i"));
    assertFalse(doc3.has("devid_i"));
    assertEquals(DoubleNode.valueOf(3.5), doc3.getJson("price_d"));
    assertEquals(created.toInstant(), doc3.getInstant("created_dt"));
    assertEquals(JsonNodeFactory.instance.arrayNode().add(3).add(13), doc3.getJson("counts_is"));
    assertEquals(List.of("a3", "b3"), doc3.getStringList("tags_ss"));
    // Solr returns _version_ (a long) on every document, which used to fail the run
    assertTrue(doc3.getJson("_version_") instanceof LongNode);
  }

  @Test
  public void testConnectorFieldsDifferingOnlyInCase() throws Exception {
    SolrInputDocument input = new SolrInputDocument("id", "doc0");
    // Solr field names are case-sensitive, so these are two fields
    input.setField("Name_s", "upper");
    input.setField("name_s", "lower");
    input.setField("devId_i", 7);
    solrClient().add(input);
    solrClient().commit();

    Document doc = queryWithConnector(Map.of());
    assertEquals("upper", doc.getString("Name_s"));
    assertEquals("lower", doc.getString("name_s"));
    assertEquals(IntNode.valueOf(7), doc.getJson("devId_i"));

    doc = queryWithConnector(Map.of("lowercaseFields", true));
    assertFalse(doc.has("Name_s"));
    assertFalse(doc.has("devId_i"));
    assertEquals(IntNode.valueOf(7), doc.getJson("devid_i"));
    // the two collapse into one field, holding one of their values
    assertEquals(1, doc.getStringList("name_s").size());
    assertTrue(List.of("upper", "lower").contains(doc.getString("name_s")));
  }

  // runs the connector over the core, expecting it to hold one document
  private Document queryWithConnector(Map<String, Object> extra) throws Exception {
    Map<String, Object> map = baseConfig("solr");
    map.put("name", "connector1");
    map.put("pipeline", "pipeline1");
    map.put("solrParams", Map.of("q", "*:*"));
    map.putAll(extra);
    Config config = ConfigFactory.parseMap(map);

    TestMessenger messenger = new TestMessenger();
    SolrConnector connector = new SolrConnector(config);
    try {
      connector.execute(new PublisherImpl(config, messenger, "run", "pipeline1"));
    } finally {
      connector.close();
    }

    List<Document> published = messenger.getDocsSentForProcessing();
    assertEquals(1, published.size());
    return published.get(0);
  }
}
