package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.net.http.HttpClient;
import java.net.http.HttpResponse;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

// TODO: a test with non-string params?
// TODO: Make sure to do a test with extra and also missing param values
// TODO: make sure to do a test with default values
// TODO: Test with a bad template response
public class QueryOpensearchTest {

  private final StageFactory factory = StageFactory.of(QueryOpensearch.class);

  private final String neckCreekResponse = """
{"took":4,"timed_out":false,"_shards":{"total":1,"successful":1,"skipped":0,"failed":0},"hits":{"total":{"value":1,"relation":"eq"},"max_score":6.7708125,"hits":[{"_index":"parks","_id":"park_dataset.csv-50","_score":6.7708125,"_source":{"id":"park_dataset.csv-50","source":"/Users/Downloads/park_dataset.csv","filename":"park_dataset.csv","park_name":"Neck Creek Preserve","sanctuary_name":"","borough":"Staten Island","acres":"20","directions":"Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.","description":"Site Description Coming Soon","habitat_type":"Salt Marsh","last_modified":"2020-03-15","csvLineNumber":50,"run_id":"9b367227-e9b9-4ae2-bccd-3f13664f7db4"}}]}}""";

  @Test
  public void testSearchTemplate() throws Exception {
    HttpResponse mockTemplateResponse = mock(HttpResponse.class);
    when(mockTemplateResponse.statusCode()).thenReturn(200);

    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      // First request is registering the template
      // Second request/response is executing the query on the document
      when(mockClient.send(any(), any()))
          .thenReturn(mockTemplateResponse)
          .thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/searchTemplate.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("response"));
    }
  }

  @Test
  public void testDestinationField() throws Exception {
    HttpResponse mockTemplateResponse = mock(HttpResponse.class);
    when(mockTemplateResponse.statusCode()).thenReturn(200);

    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any()))
          .thenReturn(mockTemplateResponse)
          .thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/specialDestination.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("special_destination"));
    }
  }

  @Test
  public void testDefaultResponseField() throws Exception {
    HttpResponse mockTemplateResponse = mock(HttpResponse.class);
    when(mockTemplateResponse.statusCode()).thenReturn(200);

    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any()))
          .thenReturn(mockTemplateResponse)
          .thenReturn(mockQueryResponse);

      Stage stage = factory.get("QueryOpensearchTest/defaultResponseField.conf");

      Document testDoc = Document.create("neck_creek");
      testDoc.setField("park_name", "Neck Creek Preserve");
      stage.processDocument(testDoc);

      assertEquals(neckCreekResponse, testDoc.getString("response"));
    }
  }

  @Test
  public void testArrayExtraction() throws Exception {
    HttpResponse mockTemplateResponse = mock(HttpResponse.class);
    when(mockTemplateResponse.statusCode()).thenReturn(200);

    HttpResponse mockQueryResponse = mock(HttpResponse.class);
    when(mockQueryResponse.body()).thenReturn(neckCreekResponse);

    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);
      when(mockClient.send(any(), any()))
          .thenReturn(mockTemplateResponse)
          .thenReturn(mockQueryResponse);

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
  public void testBadConfs() {
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/badResponsePath.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noOpensearch.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noParamNames.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noTemplate.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noTemplateName.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/partialOpensearch.conf"));
  }
}
