package com.kmwllc.lucille.util;

import static com.kmwllc.lucille.util.ElasticsearchUtils.getAllowInvalidCert;
import static com.kmwllc.lucille.util.ElasticsearchUtils.getElasticsearchRestClient;
import static com.kmwllc.lucille.util.ElasticsearchUtils.getElasticsearchUrl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.ElasticsearchTransport;
import nl.altindag.ssl.SSLFactory;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;

import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.http.HttpHost;
import java.util.Map;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ElasticsearchUtilsTest {

  @Test
  public void testGetAllowInvalidCert() {
    Map<String, Object> m = new HashMap<>();
    m.put("elasticsearch.acceptInvalidCert", "TRUE");
    Config allCaps = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    m.put("elasticsearch.acceptInvalidCert", "true");
    Config allLower = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    m.put("elasticsearch.acceptInvalidCert", "false");
    Config allLowerFalse = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    m.put("elasticsearch.acceptInvalidCert", "FALSE");
    Config allCapsFalse = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    Config none = ConfigFactory.parseMap(m);

    assertTrue(getAllowInvalidCert(allCaps));
    assertTrue(getAllowInvalidCert(allLower));
    assertFalse(getAllowInvalidCert(allLowerFalse));
    assertFalse(getAllowInvalidCert(allCapsFalse));
  }

  @Test
  public void testGetElasticsearchUrl() {
    Map<String, Object> m = new HashMap<>();
    m.put("elasticsearch.url", "foo");
    Config foo = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    Config nothing = ConfigFactory.parseMap(m);

    assertEquals("foo", getElasticsearchUrl(foo));
    assertThrows(Exception.class, () -> getElasticsearchUrl(nothing));
  }

  @Test
  public void testGetElasticsearchIndex() {
    Map<String, Object> m = new HashMap<>();
    m.put("elasticsearch.index", "foo");
    Config foo = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    Config nothing = ConfigFactory.parseMap(m);

    assertEquals("foo", ElasticsearchUtils.getElasticsearchIndex(foo));
    assertThrows(Exception.class, () -> ElasticsearchUtils.getElasticsearchIndex(nothing));
  }

  @Test
  public void testGetElasticsearchOfficialClient() {
    Config config = mock(Config.class);
    String url = "http://user:pass@localhost:9200";
    RestClient restClient = mock(RestClient.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(false);

    try (MockedStatic<RestClient> mockRestClient = mockStatic(RestClient.class);
        MockedStatic<SSLFactory> mockSSLFactory = mockStatic(SSLFactory.class)) {
      RestClientBuilder builder = mock(RestClientBuilder.class);

      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      mockRestClient.when(() -> RestClient.builder(hostCaptor.capture())).thenReturn(builder);
      when(builder.setHttpClientConfigCallback(any())).thenReturn(builder);
      when(builder.build()).thenReturn(restClient);

      SSLFactory.Builder mockSSLBuilder = mock(SSLFactory.Builder.class);
      mockSSLFactory.when(SSLFactory::builder).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.withDefaultTrustMaterial()).thenReturn(mockSSLBuilder);
      SSLFactory sslFactory = mock(SSLFactory.class);
      when(mockSSLBuilder.build()).thenReturn(sslFactory);

      ElasticsearchClient result = ElasticsearchUtils.getElasticsearchOfficialClient(config);

      assertNotNull(result);
      // check that the host has correctly been parsed
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      // since allow invalid cert is false, will not call .withTrustingAllCertificatesWithoutValidation()
      verify(mockSSLBuilder, times(1)).withDefaultTrustMaterial();
      verify(mockSSLBuilder, times(0)).withTrustingAllCertificatesWithoutValidation();

      // verify that setting up of client was called once
      verify(builder, times(1)).setHttpClientConfigCallback(any());
    }
  }

  @Test
  public void testGetElasticsearchOfficialClientAllowCert() {
    Config config = mock(Config.class);
    when (config.getString("elasticsearch.acceptInvalidCert")).thenReturn("true");
    String url = "http://user:pass@localhost:9200";
    RestClient restClient = mock(RestClient.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(true);

    try (MockedStatic<RestClient> mockRestClient = mockStatic(RestClient.class);
        MockedStatic<SSLFactory> mockSSLFactory = mockStatic(SSLFactory.class)) {
      RestClientBuilder builder = mock(RestClientBuilder.class);

      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      mockRestClient.when(() -> RestClient.builder(hostCaptor.capture())).thenReturn(builder);
      when(builder.setHttpClientConfigCallback(any())).thenReturn(builder);
      when(builder.build()).thenReturn(restClient);

      SSLFactory.Builder sslBuilder = mock(SSLFactory.Builder.class);
      mockSSLFactory.when(SSLFactory::builder).thenReturn(sslBuilder);
      when(sslBuilder.withTrustingAllCertificatesWithoutValidation()).thenReturn(sslBuilder);
      when(sslBuilder.withHostnameVerifier(any())).thenReturn(sslBuilder);
      SSLFactory sslFactory = mock(SSLFactory.class);
      when(sslBuilder.build()).thenReturn(sslFactory);

      ElasticsearchClient result = ElasticsearchUtils.getElasticsearchOfficialClient(config);

      assertNotNull(result);

      // check that the host has correctly been parsed
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      // since allow invalid cert is true, will call .withTrustingAllCertificatesWithoutValidation() and not .withDefaultTrustMaterial
      verify(sslBuilder, times(0)).withDefaultTrustMaterial();
      verify(sslBuilder, times(1)).withTrustingAllCertificatesWithoutValidation();

      // verify that setting up of client was called once
      verify(builder, times(1)).setHttpClientConfigCallback(any());
    }
  }

  @Test
  public void testGetElasticsearchOfficialClientInvalidUrl() {
    Config config = mock(Config.class);
    String invalidUrl = "invalid-url";
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(invalidUrl);

    // will throw error if config contains invalid url
    assertThrows(IllegalArgumentException.class, () -> {
      ElasticsearchUtils.getElasticsearchOfficialClient(config);
    });
  }
}
