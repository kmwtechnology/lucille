package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
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

public class QueryOpensearchTest {

  private final StageFactory factory = StageFactory.of(QueryOpensearch.class);

  String neckCreekResponse = """
{
  "took": 4,
  "timed_out": false,
  "_shards": {
    "total": 1,
    "successful": 1,
    "skipped": 0,
    "failed": 0
  },
  "hits": {
    "total": {
      "value": 1,
      "relation": "eq"
    },
    "max_score": 6.7708125,
    "hits": [
      {
        "_index": "parks",
        "_id": "park_dataset.csv-50",
        "_score": 6.7708125,
        "_source": {
          "id": "park_dataset.csv-50",
          "source": "/Users/jsquatrito/Downloads/park_dataset.csv",
          "filename": "park_dataset.csv",
          "park_name": "Neck Creek Preserve",
          "sanctuary_name": "",
          "borough": "Staten Island",
          "acres": "20",
          "directions": "Public Transit: From the Staten Island Ferry, take the 46 or 96 buses, which eventually run along South Ave to West Shore Plaza (last stop).  From the plaza, walk along South Ave to Meredith Ave.  Make a left on Meredith and the preserve is half way down the block on the right.By Car: From the Staten Island Expressway (278) exit onto 440 south toward the outer bridge crossing.  Take the first exit at South Ave.  Make a left onto Chelsea Road and then the first right onto South Ave.  Make a left on Meredith Ave.  The preserve is half way down the block on the right.",
          "description": "Site Description Coming Soon",
          "habitat_type": "Salt Marsh",
          "last_modified": "2020-03-15",
          "csvLineNumber": 50,
          "run_id": "9b367227-e9b9-4ae2-bccd-3f13664f7db4"
        }
      }
    ]
  }
}
""";

  String highRockResponse = """
{
   "took": 3,
   "timed_out": false,
   "_shards": {
     "total": 1,
     "successful": 1,
     "skipped": 0,
     "failed": 0
   },
   "hits": {
     "total": {
       "value": 1,
       "relation": "eq"
     },
     "max_score": 7.170638,
     "hits": [
       {
         "_index": "parks",
         "_id": "park_dataset.csv-20",
         "_score": 7.170638,
         "_source": {
           "id": "park_dataset.csv-20",
           "source": "/Users/jsquatrito/Downloads/park_dataset.csv",
           "filename": "park_dataset.csv",
           "park_name": "High Rock Park Preserve",
           "sanctuary_name": "",
           "borough": "Staten Island",
           "acres": "90",
           "directions": "By Subway: Trains W, R to Whitehall Street/South Ferry or 1,9 to South Ferry or 4,5 to Bowling Green.  Then take the Staten Island Ferry.  From the ferry, take bus S61, S62 or S66 to the intersections of Victory Blvd and Manor Rd.  Transfer to the S54 bus on Manor Rd and exit at Nevada Ave.  Walk up Nevada Ave. through the parking lot and up the hill to the park.By Car: From Verrazano Bridge: Take the SI expressway (route 278) west to Slosson Ave/ Todt Hill Rd exit.  Make a left at the 2nd traffic light onto Manor Rd.  Take Manor Rd. to Rockland Ave and make a left.  Proceed on Rockland Ave and make a left onto Nevada Ave.  Park is at the end of the road.From South Shore:  Take Arthur Kill Rd to Richmond town and turn right onto Richmond Rd.  Proceed to Richmond Rd to Rockland Ave (2nd light).  Turn right at Nevada Ave and continue up the hill to the parking lot.",
           "description": "Often referred to as one of the most tranquil places in New York City, High Rock Park is noted for its quiet ponds and deep woods.  High Rock Park is one of nine parks in Staten Islandís 2,500 acre Greenbelt and has been recognized as a Natural Environmental Education Landmark.  The variety of habitats, including forests, meadows and freshwater wetlands support a diverse array of plants and wildlife. There are six walking trails in the park along which visitors can see stands of red maples (Acer rubrum), Highbush blueberries (Vaccinium corymbosum) and patches of skunk cabbage (Symplocarpus foetidus). Dense forests of red maple, American beech, oaks and hickory prevent much understory plant growth, though in the spring, pinxter azalea (Rhododendron canescens) is abundant.  Visitors can also view a rare grove of persimmon (Diosyros virginiana), a tree more common to the South.   High Rock Park contains five ponds and various wetlands, including Stump Pond, Hourglass Pond, Walker Pond and Loosestrife Swamp.  Wood ducks (Aix sponsa), great blue herons (Ardrea herodias), and muskrats (Ondatra zibethica) all make their homes here along with hawks, owls, migrating warblers, woodpeckers, frogs and turtles.",
           "habitat_type": "Forest, Freshwater Wetland",
           "last_modified": "2020-12-01",
           "csvLineNumber": 20,
           "run_id": "9b367227-e9b9-4ae2-bccd-3f13664f7db4"
         }
       }
     ]
   }
}
""";

  String neckCreekQuery = """
{
  "query": {
    "match_phrase": {
      "park_name": "Neck Creek Preserve"
    }
  }
}
""";

  @Test
  public void testOpensearchQuery() throws Exception {
    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      HttpResponse mockResponse = mock(HttpResponse.class);
      when(mockResponse.body()).thenReturn(neckCreekResponse);
      when(mockClient.send(any(), any())).thenReturn(mockResponse);

      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);

      Stage stage = factory.get("QueryOpensearchTest/opensearchQuery.conf");

      Document testDoc = Document.create("test_doc");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("response"));
    }
  }

  @Test
  public void testDocumentQueryField() throws Exception {
    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);

      HttpResponse mockNeckCreekResponse = mock(HttpResponse.class);
      when(mockNeckCreekResponse.body()).thenReturn(neckCreekResponse);

      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);

      when(mockClient.send(any(), any())).thenReturn(mockNeckCreekResponse);

      Stage stage = factory.get("QueryOpensearchTest/documentQuery.conf");

      Document hasQuery = Document.create("test_doc");
      hasQuery.setField("query", neckCreekQuery);

      Document noQuery = Document.create("test_doc_no_query");

      stage.processDocument(hasQuery);
      stage.processDocument(noQuery);

      assertEquals("6.7708125", hasQuery.getString("response"));
      assertFalse(noQuery.has("response"));
    }
  }

  @Test
  public void testTwoQueries() throws Exception {
    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);

      HttpResponse mockNeckCreekResponse = mock(HttpResponse.class);
      when(mockNeckCreekResponse.body()).thenReturn(neckCreekResponse);

      HttpResponse mockHighRockResponse = mock(HttpResponse.class);
      when(mockHighRockResponse.body()).thenReturn(highRockResponse);

      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);

      // The opensearchQuery is for High Rock Park Preserve.
      // That'll be returned first, in the start() method.
      // Then the neckCreekResponse is returned second, in processDocument(),
      // on the hasQuery document.
      when(mockClient.send(any(), any()))
          .thenReturn(mockHighRockResponse)
          .thenReturn(mockNeckCreekResponse);

      Stage stage = factory.get("QueryOpensearchTest/twoQueries.conf");

      Document hasQuery = Document.create("test_doc");
      hasQuery.setField("query", neckCreekQuery);

      Document noQuery = Document.create("test_doc_no_query");

      stage.processDocument(hasQuery);
      stage.processDocument(noQuery);

      assertTrue(hasQuery.has("response"));
      assertTrue(noQuery.has("response"));

      assertEquals("6.7708125", hasQuery.getString("response"));
      assertEquals("7.170638", noQuery.getString("response"));
    }
  }

  @Test
  public void testDestinationField() throws Exception {
    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      HttpResponse mockResponse = mock(HttpResponse.class);
      when(mockResponse.body()).thenReturn(neckCreekResponse);
      when(mockClient.send(any(), any())).thenReturn(mockResponse);

      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);

      Stage stage = factory.get("QueryOpensearchTest/specialDestination.conf");

      Document testDoc = Document.create("test_doc");
      stage.processDocument(testDoc);

      assertEquals("6.7708125", testDoc.getString("special_destination"));
    }
  }

  @Test
  public void testDefaultResponseField() throws Exception {
    try (MockedStatic<HttpClient> client = Mockito.mockStatic(HttpClient.class)) {
      HttpClient mockClient = mock(HttpClient.class);
      HttpResponse mockResponse = mock(HttpResponse.class);
      when(mockResponse.body()).thenReturn(neckCreekResponse);
      when(mockClient.send(any(), any())).thenReturn(mockResponse);

      client.when(() -> HttpClient.newHttpClient()).thenReturn(mockClient);

      Stage stage = factory.get("QueryOpensearchTest/defaultResponseField.conf");

      Document testDoc = Document.create("test_doc");
      stage.processDocument(testDoc);

      assertEquals(neckCreekResponse, testDoc.getString("response"));
    }
  }

  @Test
  public void testBadConfs() {
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noOpensearch.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/noQueries.conf"));
    assertThrows(StageException.class, () -> factory.get("QueryOpensearchTest/badConfs/partialOpensearch.conf"));
  }
}
