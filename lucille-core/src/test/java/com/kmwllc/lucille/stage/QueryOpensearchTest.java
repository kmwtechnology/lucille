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
import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch.generic.Body;
import org.opensearch.client.opensearch.generic.OpenSearchGenericClient;
import org.opensearch.client.opensearch.generic.Response;

public class QueryOpensearchTest {

  private final StageFactory factory = StageFactory.of(QueryOpensearch.class);

  private final String neckCreekResponse = """
{"took":4,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":6.7708125,"hits":[{"_index":"parks","_id":"park_dataset.csv-50","_score":6.7708125,"_source":{"id":"park_dataset.csv-50","source":"/Users/Downloads/park_dataset.csv","filename":"park_dataset.csv","park_name":"Neck Creek Preserve","sanctuary_name":"","borough":"Staten Island","acres":"20","directions":"Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.","description":"Site Description Coming Soon","habitat_type":"Salt Marsh","last_modified":"2020-03-15","csvLineNumber":50,"run_id":"9b367227-e9b9-4ae2-bccd-3f13664f7db4"}}]}}""";

  private OpenSearchClient mockClient;

  @Before
  public void setup() throws IOException {
    Body mockBody = mock(Body.class);
    when(mockBody.bodyAsString()).thenReturn(neckCreekResponse);

    Response mockResponse = mock(Response.class);
    when(mockResponse.getBody()).thenReturn(Optional.of(mockBody));

    OpenSearchGenericClient mockGeneric = mock(OpenSearchGenericClient.class);
    when(mockGeneric.execute(any())).thenReturn(mockResponse);

    mockClient = mock(OpenSearchClient.class);
    when(mockClient.generic()).thenReturn(mockGeneric);
  }

  @Test
  public void testSearchTemplate() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/searchTemplate.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    assertEquals(Double.valueOf(6.7708125), testDoc.getDouble("response"));
  }

  @Test
  public void testDestinationField() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/specialDestination.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    assertEquals(Double.valueOf(6.7708125), testDoc.getDouble("special_destination"));
  }

  @Test
  public void testDefaultResponseField() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/defaultResponseField.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    assertEquals(neckCreekResponse, testDoc.getJson("response").toString());
  }

  @Test
  public void testArrayExtraction() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/arrayExtraction.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    String firstHit = """
{"_index":"parks","_id":"park_dataset.csv-50","_score":6.7708125,"_source":{"id":"park_dataset.csv-50","source":"/Users/Downloads/park_dataset.csv","filename":"park_dataset.csv","park_name":"Neck Creek Preserve","sanctuary_name":"","borough":"Staten Island","acres":"20","directions":"Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.","description":"Site Description Coming Soon","habitat_type":"Salt Marsh","last_modified":"2020-03-15","csvLineNumber":50,"run_id":"9b367227-e9b9-4ae2-bccd-3f13664f7db4"}}""";

    assertEquals(firstHit, testDoc.getJson("response").toString());
  }

  @Test
  public void testTemplateName() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/templateName.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    assertEquals(Double.valueOf(6.7708125), testDoc.getDouble("response"));
  }

  @Test
  public void testMissingRequiredField() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/searchTemplate.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    stage.processDocument(testDoc);

    assertFalse(testDoc.has("response"));
    assertTrue(testDoc.has("queryOpensearchError"));
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
  public void testBadResponsePath() throws Exception {
    QueryOpensearch stage = (QueryOpensearch) factory.get("QueryOpensearchTest/badResponsePath.conf");
    stage.setClient(mockClient);

    Document testDoc = Document.create("neck_creek");
    testDoc.setField("park_name", "Neck Creek Preserve");
    stage.processDocument(testDoc);

    // Should have an empty response, since the pointer is an invalid path, but there is no Exception here.
    assertEquals("", testDoc.getJson("response").toString());
  }
}
