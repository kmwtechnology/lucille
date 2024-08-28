package com.kmwllc.lucille.indexer;

import co.elastic.clients.elasticsearch.nodes.Http;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Event;
import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.net.Authenticator;
import java.net.CookieHandler;
import java.net.ProxySelector;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpHeaders;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpResponse.BodyHandler;
import java.net.http.HttpResponse.BodyHandlers;
import java.net.http.HttpResponse.PushPromiseHandler;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSession;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.apache.http.HttpRequestFactory;
import org.apache.http.HttpResponseFactory;
import org.apache.http.HttpStatus;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.solr.common.SolrInputDocument;
import org.jose4j.http.Response;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;


public class SearchStaxIndexerTest {

  private static final String USERNAME = "username";
  private static final String PASSWORD = "password";

  private final Config config = ConfigFactory.empty()
      .withValue("indexer.url", ConfigValueFactory.fromAnyRef("http://localhost:8089"))
      .withValue("indexer.defaultCollection", ConfigValueFactory.fromAnyRef("collection"))
      .withValue("indexer.userName", ConfigValueFactory.fromAnyRef(USERNAME))
      .withValue("indexer.password", ConfigValueFactory.fromAnyRef(PASSWORD));

  private final Document document1 =
      Document.create("doc1", "test_run");
  private final Document document2 =
      Document.create("doc2", "test_run");
  private final TestMessenger testMessenger = new TestMessenger();

  @Test
  public void testBatchSize1() throws Exception {

    Config config = this.config.withValue("indexer.batchSize", ConfigValueFactory.fromAnyRef(1));
    HttpClient httpClient = mock(HttpClient.class);

    SearchStaxIndexer indexer = new SearchStaxIndexer(config, testMessenger, false, "");
    testMessenger.sendForIndexing(document1);
    testMessenger.sendForIndexing(document2);
    indexer.run(2);

    // Each batch should be sent to Solr via a call to solrClient.add()
    // So, given 2 docs and a batch size of 1, there should be 2 batches and 2 calls to add()
    ArgumentCaptor<HttpRequest> captor = ArgumentCaptor.forClass(HttpRequest.class);
    verify(httpClient, times(2)).send(captor.capture(), any());
    assertEquals(2, captor.getAllValues().size());
    assertEquals(document1.getId(), getCapturedID(captor, 0, 0));
    assertEquals(document2.getId(), getCapturedID(captor, 1, 0));

    assertEquals(2, testMessenger.getSentEvents().size());

    List<Event> events = testMessenger.getSentEvents();
    for (int i = 1; i <= events.size(); i++) {
      assertEquals("doc" + i, events.get(i - 1).getDocumentId());
      assertEquals(Event.Type.FINISH, events.get(i - 1).getType());
    }
  }

  @Test
  public void testBatched() {

  }

  @Test
  public void testTokenAuthorization() {

  }

  @Test
  public void testFailedConnection() {

  }

  @Test
  public void testUploadError() {

  }

  private static String getCapturedID(ArgumentCaptor<HttpRequest> captor, int index, int arrIndex) {
    HttpRequest request = captor.getAllValues().get(index);

    System.out.println(request);

    return "";
  }
}
