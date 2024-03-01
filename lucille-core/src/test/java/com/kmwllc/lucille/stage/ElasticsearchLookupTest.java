package com.kmwllc.lucille.stage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import java.io.IOException;
import java.util.List;
import java.util.function.Function;
import org.apache.zookeeper.KeeperException.UnimplementedException;
import org.eclipse.jetty.util.component.Graceful.ThrowingRunnable;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.ElasticsearchUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.GetRequest;
import co.elastic.clients.elasticsearch.core.GetResponse;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.endpoints.BooleanResponse;
import co.elastic.clients.util.ObjectBuilder;

public class ElasticsearchLookupTest {

  private void assertMessage(String message, ThrowingRunnable run) throws Exception {
    try {
      run.run();
      fail("Exception not thrown");
    } catch (StageException e) {
      assertEquals(message, e.getMessage());
    }

  }

  @Test
  public void testMalformedConfigs() throws Exception {
    Config one = ConfigFactory.load("ElasticSearchLookupTest/noSource.conf");
    Config two = ConfigFactory.load("ElasticSearchLookupTest/noDest.conf");
    Config three = ConfigFactory.load("ElasticSearchLookupTest/noUrl.conf");
    Config four = ConfigFactory.load("ElasticSearchLookupTest/noIndex.conf");
    assertThrows(IllegalArgumentException.class, () -> new ElasticsearchLookup(one));
    assertThrows(IllegalArgumentException.class, () -> new ElasticsearchLookup(two));
    assertThrows(IllegalArgumentException.class, () -> new ElasticsearchLookup(three));
    assertThrows(IllegalArgumentException.class, () -> new ElasticsearchLookup(four));
  }

  @Test
  public void testStart() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/basic.conf");
    Config config2 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/basic2.conf");
    Config config3 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/basic3.conf");
    Config config4 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/basic4.conf");
    try (MockedStatic<ElasticsearchUtils> mockedUtils = Mockito.mockStatic(ElasticsearchUtils.class)) {
      ElasticsearchClient noPing = mock(ElasticsearchClient.class);
      ElasticsearchClient nullPing = mock(ElasticsearchClient.class);
      ElasticsearchClient falsePing = mock(ElasticsearchClient.class);

      BooleanResponse response = new BooleanResponse(false);
      when(nullPing.ping()).thenReturn(null);
      when(falsePing.ping()).thenReturn(response);
      when(noPing.ping()).thenThrow(new RuntimeException("could not ping"));

      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config)).thenReturn(null);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config2)).thenReturn(noPing);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config3)).thenReturn(nullPing);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config4)).thenReturn(falsePing);

      assertMessage("Client was not created.", () -> new ElasticsearchLookup(config).start());
      assertMessage("Couldn't ping elasticsearch", () -> new ElasticsearchLookup(config2).start());
      assertMessage("Non true response when pinging Elasticsearch: null", () -> new ElasticsearchLookup(config3).start());

      try {
        new ElasticsearchLookup(config4).start();
        fail("Exception not thrown");
      } catch (StageException e) {
        assertEquals("Non true response when pinging Elasticsearch: " + response.toString(), e.getMessage());
      }
    }
  }

  @Test
  public void testStop() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/default.conf");
    Config config2 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/append.conf");
    Config config3 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/overwrite.conf");
    Config config4 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/skip.conf");
    try (MockedStatic<ElasticsearchUtils> mockedUtils = Mockito.mockStatic(ElasticsearchUtils.class)) {
      ElasticsearchTransport badTransport = mock(ElasticsearchTransport.class);
      ElasticsearchTransport goodTransport = mock(ElasticsearchTransport.class);

      doThrow(new RuntimeException("did not work")).when(badTransport).close();

      ElasticsearchClient nullTransport = mock(ElasticsearchClient.class);
      ElasticsearchClient badTransportClient = mock(ElasticsearchClient.class);
      ElasticsearchClient goodTransportClient = mock(ElasticsearchClient.class);

      when(nullTransport._transport()).thenReturn(null);
      when(badTransportClient._transport()).thenReturn(badTransport);
      when(goodTransportClient._transport()).thenReturn(goodTransport);

      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config)).thenReturn(null);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config2)).thenReturn(nullTransport);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config3)).thenReturn(badTransportClient);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config4)).thenReturn(goodTransportClient);

      new ElasticsearchLookup(config).stop();
      new ElasticsearchLookup(config2).stop();
      new ElasticsearchLookup(config3).stop();
      new ElasticsearchLookup(config4).stop();

      verify(badTransport, times(1)).close();
      verify(goodTransport, times(1)).close();
    }
  }


  @Test
  public void testProcessDocument() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/throwing.conf");
    Config config2 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/noResponse.conf");
    Config config3 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/append.conf");
    Config config4 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/overwrite.conf");
    Config config5 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/skip.conf");
    Config config6 = ConfigFactory.parseResourcesAnySyntax("ElasticsearchLookupTest/default.conf");
    Function<GetRequest.Builder, ObjectBuilder<GetRequest>> fn = (g) -> g.index(null).id("doc");
    ObjectNode example = new ObjectNode(JsonNodeFactory.instance);
    example.put("foo", "bar");
    example.put("foo2", 10);
    ObjectNode base = new ObjectNode(JsonNodeFactory.instance);
    base.put("foo", "empty");
    base.put("id", "doc");
    Document doc = Document.createFromJson(base.toString());
    Document doc2 = Document.createFromJson(base.toString());
    Document doc3 = Document.createFromJson(base.toString());
    Document doc4 = Document.createFromJson(base.toString());
    Document doc5 = Document.createFromJson(base.toString());
    Document doc6 = Document.createFromJson(base.toString());
    try (MockedStatic<ElasticsearchUtils> mockedUtils = Mockito.mockStatic(ElasticsearchUtils.class)) {

      GetResponse<ObjectNode> notFound = (GetResponse<ObjectNode>) mock(GetResponse.class);
      GetResponse<ObjectNode> found = (GetResponse<ObjectNode>) mock(GetResponse.class);

      when(notFound.found()).thenReturn(false);
      when(found.found()).thenReturn(true);
      when(found.source()).thenReturn(example);

      ElasticsearchClient throwing = mock(ElasticsearchClient.class);
      ElasticsearchClient noResponse = mock(ElasticsearchClient.class);
      ElasticsearchClient foundClient = mock(ElasticsearchClient.class);

      when(throwing.get((Function<GetRequest.Builder, ObjectBuilder<GetRequest>>) Mockito.any(), Mockito.any()))
          .thenThrow(new IOException("get failed"));
      when(noResponse.get((Function<GetRequest.Builder, ObjectBuilder<GetRequest>>) Mockito.any(), (Class)Mockito.any()))
          .thenReturn(notFound);
      when(foundClient.get((Function<GetRequest.Builder, ObjectBuilder<GetRequest>>) Mockito.any(), (Class)Mockito.any()))
          .thenReturn(found);

      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config)).thenReturn(throwing);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config2)).thenReturn(noResponse);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config3)).thenReturn(foundClient);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config4)).thenReturn(foundClient);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config5)).thenReturn(foundClient);
      mockedUtils.when(() -> ElasticsearchUtils.getElasticsearchOfficialClient(config6)).thenReturn(foundClient);

      assertThrows(StageException.class, () -> {
      new ElasticsearchLookup(config).processDocument(doc);
      });
      new ElasticsearchLookup(config2).processDocument(doc2);
      new ElasticsearchLookup(config3).processDocument(doc3);
      new ElasticsearchLookup(config4).processDocument(doc4);
      new ElasticsearchLookup(config5).processDocument(doc5);
      new ElasticsearchLookup(config6).processDocument(doc6);

      assertEquals(2, doc.getFieldNames().size());
      assertEquals("empty", doc.getString("foo"));

      assertEquals(2, doc2.getFieldNames().size());
      assertEquals("empty", doc2.getString("foo"));

      assertEquals(3, doc3.getFieldNames().size());
      assertEquals(List.of("empty", "bar"), doc3.getStringList("foo"));
      assertEquals("10", doc3.getString("foo2_dest"));

      assertEquals(3, doc4.getFieldNames().size());
      assertEquals("bar", doc4.getString("foo"));
      assertEquals("10", doc4.getString("foo2_dest"));

      assertEquals(3, doc5.getFieldNames().size());
      assertEquals("empty", doc5.getString("foo"));
      assertEquals("10", doc5.getString("foo2_dest"));

      assertEquals(3, doc4.getFieldNames().size());
      assertEquals("bar", doc4.getString("foo"));
      assertEquals("10", doc4.getString("foo2_dest"));
    }
  }
}
