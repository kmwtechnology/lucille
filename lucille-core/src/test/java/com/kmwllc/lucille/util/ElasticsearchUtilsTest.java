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

import java.util.HashMap;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.ssl.TrustStrategy;
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
  public void testGetElasticsearchOfficialClient() throws Exception {
    Config config = mock(Config.class);
    String url = "http://user:pass@localhost:9200";
    Rest5Client rest5Client = mock(Rest5Client.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(false);

    try (MockedStatic<Rest5Client> mockRest5Client = mockStatic(Rest5Client.class);
        MockedStatic<SSLContextBuilder> mockSSLContextBuilder = mockStatic(SSLContextBuilder.class)) {
      Rest5ClientBuilder builder = mock(Rest5ClientBuilder.class);

      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      mockRest5Client.when(() -> Rest5Client.builder(hostCaptor.capture())).thenReturn(builder);
      when(builder.setHttpClient(any())).thenReturn(builder);
      when(builder.setCompressionEnabled(false)).thenReturn(builder);
      when(builder.build()).thenReturn(rest5Client);

      SSLContextBuilder mockSSLBuilder = mock(SSLContextBuilder.class);
      mockSSLContextBuilder.when(SSLContextBuilder::create).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.loadTrustMaterial(any(), (TrustStrategy) any())).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.build()).thenReturn(SSLContext.getDefault());


      ElasticsearchClient result = ElasticsearchUtils.getElasticsearchOfficialClient(config);

      assertNotNull(result);
      // check that the host has correctly been parsed
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      // since allow invalid cert is false, will not call .loadTrustMaterial
      verify(mockSSLBuilder, times(0)).loadTrustMaterial(any(), (TrustStrategy) any());

      // verify that setting up of client was called once
      verify(builder, times(1)).setHttpClient(any());
    }
  }

  @Test
  public void testGetElasticsearchOfficialClientAllowCert() throws Exception {
    Config config = mock(Config.class);
    when (config.getString("elasticsearch.acceptInvalidCert")).thenReturn("true");
    String url = "http://user:pass@localhost:9200";
    Rest5Client rest5Client = mock(Rest5Client.class);
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(url);
    when(ElasticsearchUtils.getAllowInvalidCert(config)).thenReturn(true);

    try (MockedStatic<Rest5Client> mockRest5Client = mockStatic(Rest5Client.class);
        MockedStatic<SSLContextBuilder> mockSSLContextBuilder = mockStatic(SSLContextBuilder.class)) {
      Rest5ClientBuilder builder = mock(Rest5ClientBuilder.class);

      ArgumentCaptor<HttpHost> hostCaptor = ArgumentCaptor.forClass(HttpHost.class);
      mockRest5Client.when(() -> Rest5Client.builder(hostCaptor.capture())).thenReturn(builder);
      when(builder.setHttpClient(any())).thenReturn(builder);
      when(builder.setCompressionEnabled(false)).thenReturn(builder);
      when(builder.build()).thenReturn(rest5Client);

      SSLContextBuilder mockSSLBuilder = mock(SSLContextBuilder.class);
      mockSSLContextBuilder.when(SSLContextBuilder::create).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.loadTrustMaterial(any(), (TrustStrategy) any())).thenReturn(mockSSLBuilder);
      when(mockSSLBuilder.build()).thenReturn(SSLContext.getDefault());

      ElasticsearchClient result = ElasticsearchUtils.getElasticsearchOfficialClient(config);

      assertNotNull(result);

      // check that the host has correctly been parsed
      assertEquals("localhost", hostCaptor.getValue().getHostName());
      assertEquals(9200, hostCaptor.getValue().getPort());
      assertEquals("http", hostCaptor.getValue().getSchemeName());

      // since allow invalid cert is true, will call .loadTrustMaterial
      verify(mockSSLBuilder, times(1)).loadTrustMaterial(any(), (TrustStrategy) any());

      // verify that setting up of client was called once
      verify(builder, times(1)).setHttpClient(any());
    }
  }

  @Test
  public void testGetElasticsearchOfficialClientInvalidUrl() {
    Config config = mock(Config.class);
    String invalidUrl = "invalid-url";
    when(ElasticsearchUtils.getElasticsearchUrl(config)).thenReturn(invalidUrl);

    // will throw error if config contains invalid url
    assertThrows(Exception.class, () -> {
      ElasticsearchUtils.getElasticsearchOfficialClient(config);
    });
  }
}
