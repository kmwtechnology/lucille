package com.kmwllc.lucille.indexer;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.embedded.JettySolrRunner;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;

/**
 * Contains test that verify the integration with a working MiniSolrCloudCluster using the
 * SolrTestFramework.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public class SolrIndexerIntegrationTest extends SolrCloudTestCase {

  private static MiniSolrCloudCluster cluster;

  private static final String COL = "test";

  @BeforeClass
  public static void setupCluster() throws Exception {
    cluster =
        configureCluster(1)
            .addConfig(
                COL,
                Path.of(
                    SolrIndexerIntegrationTest.class
                        .getClassLoader()
                        .getResource("SolrIndexerIntegrationTest/configsets/test/conf")
                        .getPath()))
            .configure();

    CollectionAdminRequest.createCollection(COL, COL, 1, 1).process(cluster.getSolrClient());
  }

  @Before
  public void setup() throws SolrServerException, IOException {
    cluster.getSolrClient().deleteByQuery(COL, "*:*");
    cluster.getSolrClient().commit(COL);
  }

  @Test
  public void testDeleteById() throws Exception {
    new UpdateRequest()
        .add(
            Arrays.asList(
                new SolrInputDocument(
                    "id", "id_1",
                    "delete_by_id", "deleteid_1"),
                new SolrInputDocument(
                    "id", "id_2",
                    "delete_by_id", "deleteid_1")))
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
        .process(cluster.getSolrClient(), COL);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", jetty.getBaseUrl().toString());
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 1);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_deleted");
    map.put("indexer.deletionMarkerFieldValue", "true");

    Config config = ConfigFactory.parseMap(map);

    SolrIndexer indexer = null;
    IndexerMessageManager mockIndexerMessageManager = mock(IndexerMessageManager.class);

    try {
      indexer = new SolrIndexer(config, mockIndexerMessageManager, false, "solr");
      assertTrue(indexer.validateConnection());

      SolrQuery qr = new SolrQuery();
      qr.set("q", "*:*");

      assertEquals(
          "The test documents should have been indexed.",
          2,
          cluster.getSolrClient().query(COL, qr).getResults().size());

      Document delete = Document.create("id_1");
      delete.setField("is_deleted", "true");
      delete.setField("collection", COL);
      indexer.sendToIndex(List.of(delete));

      cluster.getSolrClient().commit(COL);
      assertEquals(
          "One of the test documents should have been deleted",
          1,
          cluster.getSolrClient().query(COL, qr).getResults().size());

    } finally {
      if (indexer != null) {
        indexer.closeConnection();
      }
    }
  }

  @Test
  public void testDeleteByField() throws Exception {
    new UpdateRequest()
        .add(
            Arrays.asList(
                new SolrInputDocument(
                    "id", "id_1",
                    "delete_by_id", "deleteid_1"),
                new SolrInputDocument(
                    "id", "id_2",
                    "delete_by_id", "deleteid_2")))
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
        .process(cluster.getSolrClient(), COL);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", jetty.getBaseUrl().toString());
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 1);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_deleted");
    map.put("indexer.deletionMarkerFieldValue", "true");
    map.put("indexer.deleteByFieldField", "delete_by_field");
    map.put("indexer.deleteByFieldValue", "delete_by_id");

    Config config = ConfigFactory.parseMap(map);

    SolrIndexer indexer = null;
    IndexerMessageManager mockIndexerMessageManager = mock(IndexerMessageManager.class);

    try {
      indexer = new SolrIndexer(config, mockIndexerMessageManager, false, "solr");
      assertTrue(indexer.validateConnection());

      SolrQuery qr = new SolrQuery();
      qr.set("q", "*:*");

      assertEquals(
          "The test documents should have been indexed.",
          2,
          cluster.getSolrClient().query(COL, qr).getResults().size());

      Document delete = Document.create("id_2");
      delete.setField("is_deleted", "true");
      delete.setField("delete_by_field", "delete_by_id");
      delete.setField("delete_by_id", "deleteid_1");
      delete.setField("collection", COL);
      indexer.sendToIndex(List.of(delete));

      cluster.getSolrClient().commit(COL);
      assertEquals(
          "One of the test documents should have been deleted",
          1,
          cluster.getSolrClient().query(COL, qr).getResults().size());

    } finally {
      if (indexer != null) {
        indexer.closeConnection();
      }
    }
  }

  @Test
  public void testDeleteByIdsAndFields() throws Exception {
    new UpdateRequest()
        .add(
            Arrays.asList(
                new SolrInputDocument(
                    "id", "id_1",
                    "delete_by_id", "deleteid_1"),
                new SolrInputDocument(
                    "id", "id_2",
                    "delete_by_id", "deleteid_2"),
                new SolrInputDocument(
                    "id", "id_3",
                    "delete_by_id", "deleteid_2"),
                new SolrInputDocument(
                    "id", "id_4",
                    "delete_by_id", "deleteid_4")))
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
        .process(cluster.getSolrClient(), COL);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", jetty.getBaseUrl().toString());
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 2);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_deleted");
    map.put("indexer.deletionMarkerFieldValue", "true");
    map.put("indexer.deleteByFieldField", "delete_by_field");
    map.put("indexer.deleteByFieldValue", "delete_by_id");

    Config config = ConfigFactory.parseMap(map);

    SolrIndexer indexer = null;
    IndexerMessageManager mockIndexerMessageManager = mock(IndexerMessageManager.class);

    try {
      indexer = new SolrIndexer(config, mockIndexerMessageManager, false, "solr");
      assertTrue(indexer.validateConnection());

      SolrQuery qr = new SolrQuery();
      qr.set("q", "*:*");

      assertEquals(
          "The test documents should have been indexed.",
          4,
          cluster.getSolrClient().query(COL, qr).getResults().size());

      Document delete1 = Document.create("id_2");
      delete1.setField("is_deleted", "true");
      delete1.setField("delete_by_field", "delete_by_id");
      delete1.setField("delete_by_id", "deleteid_1");
      delete1.setField("collection", COL);

      Document delete2 = Document.create("id_2");
      delete2.setField("is_deleted", "true");
      delete2.setField("delete_by_field", "delete_by_id");
      delete2.setField("delete_by_id", "deleteid_2");
      delete2.setField("collection", COL);

      indexer.sendToIndex(List.of(delete1, delete2));

      cluster.getSolrClient().commit(COL);
      QueryResponse response = cluster.getSolrClient().query(COL, qr);
      assertEquals(
          "The test documents except for one should have been deleted",
          1,
          response.getResults().size());
      assertEquals(
          "The test document that was not deleted should be id_4",
          "id_4",
          response.getResults().get(0).getFirstValue("id"));

    } finally {
      if (indexer != null) {
        indexer.closeConnection();
      }
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
