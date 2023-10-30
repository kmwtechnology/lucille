package com.kmwllc.lucille.indexer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.message.IndexerMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
import org.apache.solr.client.solrj.request.AbstractUpdateRequest;
import org.apache.solr.client.solrj.request.CollectionAdminRequest;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.cloud.MiniSolrCloudCluster;
import org.apache.solr.cloud.SolrCloudTestCase;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.embedded.JettySolrRunner;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

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

    CollectionAdminRequest.createCollection(COL, COL, 1, 1)
        .process(cluster.getSolrClient());
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
    map.put("solr.url", Arrays.asList(jetty.getBaseUrl().toString()));
    map.put("solr.useCloudClient", true);
    map.put("solr.defaultCollection", "test");
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 1);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_delete");
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
      delete.setField("is_delete", "true");
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
                    "myField", "123"),
                new SolrInputDocument(
                    "id", "id_2",
                    "myField", "321")))
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
        .process(cluster.getSolrClient(), COL);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", Arrays.asList(jetty.getBaseUrl().toString()));
    map.put("solr.useCloudClient", true);
    map.put("solr.defaultCollection", "test");
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 1);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_delete");
    map.put("indexer.deletionMarkerFieldValue", "true");
    /*
    The field on the document referenced by the deleteByFieldField contains the field name in solr that the document
    is indicating should be used in a delete by query request. The field on the document referenced by the deleteByFieldValue
    contains the value of the field that should be used in a delete by query request. When a document containing the
    deletionMarkerField set to the deletionMarkerValue that has a deleteByFieldField and a deleteByFieldValue then all
    of the documents in solr containing the value for that field will by deleted.
     */
    map.put("indexer.deleteByFieldField", "delete.FieldName");
    map.put("indexer.deleteByFieldValue", "delete.FieldValue");

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
      delete.setField("is_delete", "true");
      delete.setField("delete.FieldName", "myField");
      delete.setField("delete.FieldValue", "123");
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
                    "myField", "123"),
                new SolrInputDocument(
                    "id", "id_2",
                    "myField", "234"),
                new SolrInputDocument(
                    "id", "id_3",
                    "myField", "234"),
                new SolrInputDocument(
                    "id", "id_4",
                    "myField", "321")))
        .setAction(AbstractUpdateRequest.ACTION.COMMIT, true, true)
        .process(cluster.getSolrClient(), COL);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", Arrays.asList(jetty.getBaseUrl().toString()));
    map.put("solr.useCloudClient", true);
    map.put("solr.defaultCollection", "test");
    map.put("indexer.batchTimeout", 5000);
    map.put("indexer.batchSize", 2);
    map.put("indexer.type", "solr");
    map.put("indexer.sendEnabled", true);
    map.put("indexer.indexOverrideField", "collection");
    map.put("indexer.deletionMarkerField", "is_delete");
    map.put("indexer.deletionMarkerFieldValue", "true");
    /*
    The field on the document referenced by the deleteByFieldField contains the field name in solr that the document
    is indicating should be used in a delete by query request. The field on the document referenced by the deleteByFieldValue
    contains the value of the field that should be used in a delete by query request. When a document containing the
    deletionMarkerField set to the deletionMarkerValue that has a deleteByFieldField and a deleteByFieldValue then all
    of the documents in solr containing the value for that field will by deleted.
     */
    map.put("indexer.deleteByFieldField", "delete.FieldName");
    map.put("indexer.deleteByFieldValue", "delete.FieldValue");

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

      // Send one delete that deletes the id:id_1 document from the index.
      Document delete1 = Document.create("id_1");
      delete1.setField("is_delete", "true");
      delete1.setField("collection", COL);

      // Send a second delete that deletes all documents with a field "myField" containing "234" from the index.
      Document delete2 = Document.create("id_2");
      delete2.setField("is_delete", "true");
      delete2.setField("delete.FieldName", "myField");
      delete2.setField("delete.FieldValue", "234");
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

  @Test
  public void testValidateConnection() throws SolrServerException, IOException {
    /*
    We've found a case whereby the indexer.validateConnection tests for the SolrIndexer against the embedded Solr Server used in
    this integration test will pass in a build, but not pass against a live Solr cluster / server. Config properties such as
    defaultCollection, zkHosts, and zkChroot will not be considered in these tests and their relevant functionality
    may error in manual testing. It is recommended that when making changes to related code to perform a manual test as well.
     */

    Http2SolrClient mockHttp2Client = mock(Http2SolrClient.class);
    CloudHttp2SolrClient mockCloudHttp2Client = mock(CloudHttp2SolrClient.class);

    JettySolrRunner jetty = cluster.getJettySolrRunners().get(0);

    // Create a HTTP2SolrClient config
    Map<String, Object> mapHttp = new HashMap<>();
    mapHttp.put("solr.url", jetty.getBaseUrl().toString() + "/test");
    mapHttp.put("indexer.batchTimeout", 5000);
    mapHttp.put("indexer.batchSize", 2);
    mapHttp.put("indexer.type", "solr");
    Config configHttp = ConfigFactory.parseMap(mapHttp);

    // Create a CloudHTTP2SolrClient config
    Map<String, Object> mapCloud = new HashMap<>();
    mapCloud.put("solr.url", Arrays.asList(jetty.getBaseUrl().toString()));
    mapCloud.put("solr.useCloudClient", true);
    mapCloud.put("solr.defaultCollection", "test");
    mapCloud.put("indexer.batchTimeout", 5000);
    mapCloud.put("indexer.batchSize", 2);
    mapCloud.put("indexer.type", "solr");
    Config configCloud = ConfigFactory.parseMap(mapCloud);

    IndexerMessageManager mockIndexerMessageManager = mock(IndexerMessageManager.class);

    SolrIndexer indexer1 = new SolrIndexer(configHttp, mockIndexerMessageManager, mockHttp2Client, "solr");
    SolrIndexer indexer2 = new SolrIndexer(configCloud, mockIndexerMessageManager, mockCloudHttp2Client, "solr");

    indexer1.validateConnection();
    indexer2.validateConnection();

    verify(mockHttp2Client).ping();
    verify(mockCloudHttp2Client).request(Mockito.any(CollectionAdminRequest.ClusterStatus.class));
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (cluster != null) {
      cluster.shutdown();
    }
  }
}
