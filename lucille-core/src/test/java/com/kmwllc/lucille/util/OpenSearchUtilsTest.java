package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.security.KeyStore;
import java.util.HashMap;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.message.BasicHttpResponse;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

public class OpenSearchUtilsTest {
  @Test
  public void testGetAllowInvalidCert() {
    Map<String, String> trueMap = new HashMap<>();
    trueMap.put("opensearch.acceptInvalidCert", "true");
    Config trueConfig = ConfigFactory.parseMap(trueMap);

    Map<String, String> falseMap = new HashMap<>();
    falseMap.put("opensearch.acceptInvalidCert", "false");
    Config falseConfig = ConfigFactory.parseMap(falseMap);

    Map<String, String> trueCapsMap = new HashMap<>();
    trueCapsMap.put("opensearch.acceptInvalidCert", "TRUE");
    Config trueCapsConfig = ConfigFactory.parseMap(trueCapsMap);

    Map<String, String> falseCapsMap = new HashMap<>();
    falseCapsMap.put("opensearch.acceptInvalidCert", "FALSE");
    Config falseCapsConfig = ConfigFactory.parseMap(falseCapsMap);

    Map<String, String> emptyMap = new HashMap<>();
    Config emptyConfig = ConfigFactory.parseMap(emptyMap);

    assertTrue(OpenSearchUtils.getAllowInvalidCert(trueConfig));
    assertFalse(OpenSearchUtils.getAllowInvalidCert(falseConfig));
    assertTrue(OpenSearchUtils.getAllowInvalidCert(trueCapsConfig));
    assertFalse(OpenSearchUtils.getAllowInvalidCert(falseCapsConfig));

    assertFalse(OpenSearchUtils.getAllowInvalidCert(emptyConfig));
  }

  @Test
  public void testGetOpensearchUrl() {
    Map<String, String> testUrlMap = new HashMap<>();
    testUrlMap.put("opensearch.url", "TESTURL.com");
    Config testUrlConfig = ConfigFactory.parseMap(testUrlMap);

    Map<String, String> emptyMap = new HashMap<>();
    Config emptyConfig = ConfigFactory.parseMap(emptyMap);

    assertEquals("TESTURL.com", OpenSearchUtils.getOpenSearchUrl(testUrlConfig));
    assertThrows(Exception.class,
        () -> OpenSearchUtils.getOpenSearchUrl(emptyConfig)
    );
  }

  @Test
  public void testGetOpensearchIndex() {
    Map<String, String> testIndexMap = new HashMap<>();
    testIndexMap.put("opensearch.index", "test_index");
    Config testUrlConfig = ConfigFactory.parseMap(testIndexMap);

    Map<String, String> emptyMap = new HashMap<>();
    Config emptyConfig = ConfigFactory.parseMap(emptyMap);

    assertEquals("test_index", OpenSearchUtils.getOpenSearchIndex(testUrlConfig));
    assertThrows(Exception.class,
        () -> OpenSearchUtils.getOpenSearchIndex(emptyConfig)
    );
  }

  @Test
  public void testGetOpensearchOfficialClient() throws Exception {
    Map<String, String> testClientMap = new HashMap<>();
    testClientMap.put("opensearch.url", "http://user:pass@localhost:9200");
    testClientMap.put("opensearch.acceptInvalidCert", "false");
    Config testClientConfig = ConfigFactory.parseMap(testClientMap);

    try (MockedStatic<ApacheHttpClient5TransportBuilder> mockStaticTransportBuilder = mockStatic(ApacheHttpClient5TransportBuilder.class);
        MockedStatic<SSLContextBuilder> mockStaticSSLBuilder = mockStatic(SSLContextBuilder.class);
        MockedStatic<ClientTlsStrategyBuilder> mockStaticTlsBuilder = mockStatic(ClientTlsStrategyBuilder.class)) {
      // Setting up arg capturing + mocking for the Apache transport builder
      ApacheHttpClient5TransportBuilder mockClientBuilder = mock(ApacheHttpClient5TransportBuilder.class);
      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);

      mockStaticTransportBuilder.when(() -> ApacheHttpClient5TransportBuilder.builder(hostCaptor.capture())).thenReturn(mockClientBuilder);
      when(mockClientBuilder.setMapper(any())).thenReturn(mockClientBuilder);
      when(mockClientBuilder.setHttpClientConfigCallback(any())).thenReturn(mockClientBuilder);

      // Setting up the sslBuilder and the tlsBuilder
      SSLContextBuilder mockSSLBuilder = mock(SSLContextBuilder.class);
      SSLContext mockSSLContext = mock(SSLContext.class);
      mockStaticSSLBuilder.when(SSLContextBuilder::create).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.build()).thenReturn(mockSSLContext);

      ClientTlsStrategyBuilder mockTlsBuilder = mock(ClientTlsStrategyBuilder.class);
      TlsStrategy mockStrategy = mock(TlsStrategy.class);
      mockStaticTlsBuilder.when(ClientTlsStrategyBuilder::create).thenReturn(mockTlsBuilder);
      when(mockTlsBuilder.setSslContext(any())).thenReturn(mockTlsBuilder);
      when(mockTlsBuilder.build()).thenReturn(mockStrategy);

      assertNotNull(OpenSearchUtils.getOpenSearchRestClient(testClientConfig));

      // making sure appropriate host information
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      verify(mockClientBuilder, times(1)).setHttpClientConfigCallback(any());

      // ssl context - loadTrustMaterial not called
      // tls strategy - setHostnameVerifier not called
      verify(mockSSLBuilder, times(0)).loadTrustMaterial(any(KeyStore.class), any());
      verify(mockSSLBuilder, times(1)).build();
      verify(mockTlsBuilder, times(0)).setHostnameVerifier(any());
      verify(mockTlsBuilder, times(1)).build();
    }
  }

  @Test
  public void testGetOpensearchOfficialClientAllowInvalid() throws Exception {
    Map<String, String> testClientMap = new HashMap<>();
    testClientMap.put("opensearch.url", "http://user:pass@localhost:9200");
    testClientMap.put("opensearch.acceptInvalidCert", "true");
    Config testClientConfig = ConfigFactory.parseMap(testClientMap);

    try (MockedStatic<ApacheHttpClient5TransportBuilder> mockStaticTransportBuilder = mockStatic(ApacheHttpClient5TransportBuilder.class);
        MockedStatic<ClientTlsStrategyBuilder> mockStaticTlsBuilder = mockStatic(ClientTlsStrategyBuilder.class)) {
      // Setting up arg capturing + mocking for the Apache transport builder
      ApacheHttpClient5TransportBuilder mockClientBuilder = mock(ApacheHttpClient5TransportBuilder.class);
      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);

      mockStaticTransportBuilder.when(() -> ApacheHttpClient5TransportBuilder.builder(hostCaptor.capture())).thenReturn(mockClientBuilder);
      when(mockClientBuilder.setMapper(any())).thenReturn(mockClientBuilder);
      when(mockClientBuilder.setHttpClientConfigCallback(any())).thenReturn(mockClientBuilder);

      // Setting up the TLSBuilder. SSLBuilder couldn't be mocked for this test.
      ClientTlsStrategyBuilder mockTlsBuilder = mock(ClientTlsStrategyBuilder.class);
      TlsStrategy mockStrategy = mock(TlsStrategy.class);

      mockStaticTlsBuilder.when(ClientTlsStrategyBuilder::create).thenReturn(mockTlsBuilder);
      when(mockTlsBuilder.setSslContext(any())).thenReturn(mockTlsBuilder);
      when(mockTlsBuilder.setHostnameVerifier(any())).thenReturn(mockTlsBuilder);
      when(mockTlsBuilder.build()).thenReturn(mockStrategy);

      assertNotNull(OpenSearchUtils.getOpenSearchRestClient(testClientConfig));

      // making sure appropriate host information
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      verify(mockClientBuilder, times(1)).setHttpClientConfigCallback(any());

      // tls strategy - setHostnameVerifier called
      verify(mockTlsBuilder, times(1)).setHostnameVerifier(any());
      verify(mockTlsBuilder, times(1)).build();
    }
  }

  @Test
  public void testGetOpensearchOfficialClientInvalidUrl() {
    Map<String, String> badUrlMap = new HashMap<>();
    badUrlMap.put("opensearch.url", "bad-url");
    Config badUrlConfig = ConfigFactory.parseMap(badUrlMap);

    // Exceptions with a bad url should be caught.
    assertThrows(Exception.class, () -> OpenSearchUtils.getOpenSearchRestClient(badUrlConfig));

  }

  @Test
  public void testRetryOn429and503WithIncrementalBackoff() {
    DefaultHttpRequestRetryStrategy strategy = OpenSearchUtils.buildRetryStrategy();

    HttpResponse response429 = new BasicHttpResponse(429, "Too Many Requests");

    // On a 429, retry is allowed up to maxRetries of 3
    assertTrue(strategy.retryRequest(response429, 1, null));
    assertEquals(TimeValue.ofSeconds(2), strategy.getRetryInterval(response429, 1, null));

    assertTrue(strategy.retryRequest(response429, 2, null));
    assertEquals(TimeValue.ofSeconds(4), strategy.getRetryInterval(response429, 2, null));

    assertTrue(strategy.retryRequest(response429, 3, null));
    assertEquals(TimeValue.ofSeconds(8), strategy.getRetryInterval(response429, 3, null));

    // No more retrying after maxRetries is hit
    assertFalse(strategy.retryRequest(response429, 4, null));

    // Replicate for a 503 code
    HttpResponse response503 = new BasicHttpResponse(503, "Service Unavailable");

    assertTrue(strategy.retryRequest(response503, 1, null));
    assertEquals(TimeValue.ofSeconds(2), strategy.getRetryInterval(response429, 1, null));

    assertTrue(strategy.retryRequest(response503, 2, null));
    assertEquals(TimeValue.ofSeconds(4), strategy.getRetryInterval(response429, 2, null));

    assertTrue(strategy.retryRequest(response503, 3, null));
    assertEquals(TimeValue.ofSeconds(8), strategy.getRetryInterval(response429, 3, null));

    // No more retrying after maxRetries is hit
    assertFalse(strategy.retryRequest(response503, 4, null));

    // 200 should not retry
    HttpResponse response200 = new BasicHttpResponse(200, "OK");
    assertFalse(strategy.retryRequest(response200, 1, null));
  }

}
