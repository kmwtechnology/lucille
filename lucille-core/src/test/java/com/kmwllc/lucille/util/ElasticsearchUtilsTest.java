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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import java.net.URI;
import nl.altindag.ssl.SSLFactory;

import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
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

    assertTrue(ElasticsearchUtils.getAllowInvalidCert(allCaps));
    assertTrue(ElasticsearchUtils.getAllowInvalidCert(allLower));
    assertFalse(ElasticsearchUtils.getAllowInvalidCert(allLowerFalse));
    assertFalse(ElasticsearchUtils.getAllowInvalidCert(allCapsFalse));
  }

  @Test
  public void testGetElasticsearchUrl() {
    Map<String, Object> m = new HashMap<>();
    m.put("elasticsearch.url", "foo");
    Config foo = ConfigFactory.parseMap(m);

    m = new HashMap<>();
    Config nothing = ConfigFactory.parseMap(m);

    assertEquals("foo", ElasticsearchUtils.getElasticsearchUrl(foo));
    assertThrows(Exception.class, () -> ElasticsearchUtils.getElasticsearchUrl(nothing));
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
    Rest5Client restClient = mock(Rest5Client.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(false);

    try (MockedStatic<Rest5Client> mockRestClient = mockStatic(Rest5Client.class);
        MockedStatic<SSLFactory> mockSSLFactory = mockStatic(SSLFactory.class)) {
      Rest5ClientBuilder builder = mock(Rest5ClientBuilder.class);

      ArgumentCaptor<URI> hostURICaptor = ArgumentCaptor.forClass(URI.class);
      mockRestClient.when(() -> Rest5Client.builder(hostURICaptor.capture())).thenReturn(builder);
      when(builder.setDefaultHeaders(any())).thenReturn(builder);
      when(builder.setSSLContext(any())).thenReturn(builder);
      when(builder.build()).thenReturn(restClient);

      SSLFactory.Builder mockSSLBuilder = mock(SSLFactory.Builder.class);
      mockSSLFactory.when(SSLFactory::builder).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.withDefaultTrustMaterial()).thenReturn(mockSSLBuilder);
      SSLFactory sslFactory = mock(SSLFactory.class);
      when(mockSSLBuilder.build()).thenReturn(sslFactory);

      ElasticsearchClient result = ElasticsearchUtils.getElasticsearchOfficialClient(config);

      assertNotNull(result);
      // check that the host has correctly been parsed
      assertEquals("http://user:pass@localhost:9200", hostURICaptor.getValue().toString());

      // since allow invalid cert is false, will not call .withTrustingAllCertificatesWithoutValidation()
      verify(mockSSLBuilder, times(1)).withDefaultTrustMaterial();
      verify(mockSSLBuilder, times(0)).withTrustingAllCertificatesWithoutValidation();

      // verify that username/password was set
      verify(builder, times(1)).setDefaultHeaders(any());
    }
  }

  @Test
  public void testGetElasticsearchOfficialClientAllowCert() {
    Config config = mock(Config.class);
    when (config.getString("elasticsearch.acceptInvalidCert")).thenReturn("true");
    String url = "http://user:pass@localhost:9200";
    Rest5Client restClient = mock(Rest5Client.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(true);

    try (MockedStatic<Rest5Client> mockRestClient = mockStatic(Rest5Client.class);
        MockedStatic<SSLFactory> mockSSLFactory = mockStatic(SSLFactory.class)) {
      Rest5ClientBuilder builder = mock(Rest5ClientBuilder.class);

      ArgumentCaptor<URI> hostURICaptor = ArgumentCaptor.forClass(URI.class);
      mockRestClient.when(() -> Rest5Client.builder(hostURICaptor.capture())).thenReturn(builder);
      when(builder.setSSLContext(any())).thenReturn(builder);
      when(builder.setDefaultHeaders(any())).thenReturn(builder);
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
      assertEquals("http://user:pass@localhost:9200", hostURICaptor.getValue().toString());

      // since allow invalid cert is true, will call .withTrustingAllCertificatesWithoutValidation() and not .withDefaultTrustMaterial
      verify(sslBuilder, times(0)).withDefaultTrustMaterial();
      verify(sslBuilder, times(1)).withTrustingAllCertificatesWithoutValidation();

      // verify that username/password was set
      verify(builder, times(1)).setDefaultHeaders(any());
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
