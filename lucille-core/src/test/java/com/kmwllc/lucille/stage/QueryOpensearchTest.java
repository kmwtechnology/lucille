package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class QueryOpensearchTest {

  private final StageFactory factory = StageFactory.of(QueryOpensearch.class);

  private final String neckCreekResponse = """
{"took":4,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":6.7708125,"hits":[{"_index":"parks","_id":"park_dataset.csv-50","_score":6.7708125,"_source":{"id":"park_dataset.csv-50","source":"/Users/Downloads/park_dataset.csv","filename":"park_dataset.csv","park_name":"Neck Creek Preserve","sanctuary_name":"","borough":"Staten Island","acres":"20","directions":"Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.","description":"Site Description Coming Soon","habitat_type":"Salt Marsh","last_modified":"2020-03-15","csvLineNumber":50,"run_id":"9b367227-e9b9-4ae2-bccd-3f13664f7db4"}}]}}""";

  @Test
  public void testSearchTemplate() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      // First request is registering the template
      // Second request/response is executing the query on the document
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/searchTemplate.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("response"));
    }
  }

  @Test
  public void testDestinationField() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/specialDestination.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("special_destination"));
    }
  }

  @Test
  public void testDefaultResponseField() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/defaultResponseField.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals(neckCreekResponse, testDoc.getString("response"));
    }
  }

  @Test
  public void testArrayExtraction() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/arrayExtraction.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      String firstHit = """
{"_index":"parks","_id":"park_dataset.csv-50","_score":6.7708125,"_source":{"id":"park_dataset.csv-50","source":"/Users/Downloads/park_dataset.csv","filename":"park_dataset.csv","park_name":"Neck Creek Preserve","sanctuary_name":"","borough":"Staten Island","acres":"20","directions":"Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.","description":"Site Description Coming Soon","habitat_type":"Salt Marsh","last_modified":"2020-03-15","csvLineNumber":50,"run_id":"9b367227-e9b9-4ae2-bccd-3f13664f7db4"}}""";

      assertEquals(firstHit, testDoc.getString("response"));
    }
  }

  @Test
  public void testTemplateName() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/templateName.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("response"));
    }
  }


  @Test
  public void testMissingRequiredField() throws Exception {
    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any())).thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/searchTemplate.conf");

      Document testDoc = Document.create("neck_creek");
      stage.processDocument(testDoc);

      assertFalse(testDoc.has("response"));
      assertTrue(testDoc.has("queryOpensearchError"));
    }
  }

  @Test
  public void testBadConfs() {
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/badResponsePath.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noOpensearch.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noTemplateNoName.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/templateAndName.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/partialOpensearch.conf"));
    // a bad, non-valid JSON search template should not work
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/badTemplate.conf"));
  }

  @Test
  public void testPopulateJsonRequiredFields() throws Exception {
    // Has "park_name" as a default field
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/searchTemplate.conf");

    Document doc = Document.create("with_name");
    doc.setField("park_name", "Park Creek Preserve");

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node1 = mapper.createObjectNode();

    stage.populateJsonWithParams(doc, node1);

    assertTrue(node1.has("params"));
    assertTrue(node1.get("params").has("park_name"));
    assertEquals("Park Creek Preserve", node1.get("params").get("park_name").asText());

    ObjectNode node2 = mapper.createObjectNode();
    assertThrows(NoSuchFieldException.class, () -> stage.populateJsonWithParams(Document.create("no_field"), node2));
  }

  @Test
  public void testPopulateJsonOptionalFields() throws Exception {
    // Has "park_name" as an optional field
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/optionalFields.conf");

    Document doc = Document.create("with_name");
    doc.setField("park_name", "Park Creek Preserve");

    ObjectMapper mapper = new ObjectMapper();
    ObjectNode node1 = mapper.createObjectNode();

    stage.populateJsonWithParams(doc, node1);

    assertTrue(node1.has("params"));
    assertTrue(node1.get("params").has("park_name"));
    assertEquals("Park Creek Preserve", node1.get("params").get("park_name").asText());

    ObjectNode node2 = mapper.createObjectNode();
    stage.populateJsonWithParams(Document.create("no_field"), node2);

    // Still gets a params node but it is empty map.
    assertTrue(node2.has("params"));
    assertFalse(node2.get("params").has("park_name"));
  }

  @Test
  public void testGetTemplateURI() throws StageException {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/arrayExtraction.conf");
    Config goodConfig = ConfigFactory.load("QueryOpensearchTest/searchTemplate.conf");

    assertEquals("http://localhost:9200/parks/_search/template", stage.getTemplateSearchURI(goodConfig).toString());
  }
}
