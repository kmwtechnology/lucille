package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
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
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.core5.http.HttpHost;
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

    // ssl context - loadTrustMaterial not called
    // tls strategy - setHostnameVerifier not called
    // ^^ How to test? Since code is part of a callback function
    try (MockedStatic<ApacheHttpClient5TransportBuilder> mockTransportBuilder = mockStatic(ApacheHttpClient5TransportBuilder.class)) {
      ApacheHttpClient5TransportBuilder mockBuilder = mock(ApacheHttpClient5TransportBuilder.class);
      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      when(mockBuilder.setMapper(any())).thenReturn(mockBuilder);
      when(mockBuilder.setHttpClientConfigCallback(any())).thenReturn(mockBuilder);

      mockTransportBuilder.when(() -> ApacheHttpClient5TransportBuilder.builder(hostCaptor.capture())).thenReturn(mockBuilder);

      OpenSearchUtils.getOpenSearchRestClient(testClientConfig);

      // making sure appropriate host information
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      verify(mockBuilder, times(1)).setHttpClientConfigCallback(any());
    }
  }

  @Test
  public void testGetOpensearchOfficialClientAllowCert() throws Exception {
    Map<String, String> testClientMap = new HashMap<>();
    testClientMap.put("opensearch.url", "http://user:pass@localhost:9200");
    testClientMap.put("opensearch.acceptInvalidCert", "true");
    Config testClientConfig = ConfigFactory.parseMap(testClientMap);

    // ssl context - loadTrustMaterial called
    // tls strategy - setHostnameVerifier called
    // ^^ How to test? Since code is part of a callback function
    try (MockedStatic<ApacheHttpClient5TransportBuilder> mockTransportBuilder = mockStatic(ApacheHttpClient5TransportBuilder.class)) {
      ApacheHttpClient5TransportBuilder mockBuilder = mock(ApacheHttpClient5TransportBuilder.class);
      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      when(mockBuilder.setMapper(any())).thenReturn(mockBuilder);
      when(mockBuilder.setHttpClientConfigCallback(any())).thenReturn(mockBuilder);

      mockTransportBuilder.when(() -> ApacheHttpClient5TransportBuilder.builder(hostCaptor.capture())).thenReturn(mockBuilder);

      OpenSearchUtils.getOpenSearchRestClient(testClientConfig);

      // making sure appropriate host information
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      verify(mockBuilder, times(1)).setHttpClientConfigCallback(any());
    }
  }

  @Test
  public void testGetOpensearchOfficialClientInvalidUrl() {
    Map<String, String> badUrlMap = new HashMap<>();
    badUrlMap.put("opensearch.url", "bad-url");
    Config badUrlConfig = ConfigFactory.parseMap(badUrlMap);

    // Exceptions get caught and null is returned.
    assertNull(OpenSearchUtils.getOpenSearchRestClient(badUrlConfig));
  }
}
