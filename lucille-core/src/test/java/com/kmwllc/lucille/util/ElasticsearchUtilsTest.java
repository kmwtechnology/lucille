package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.mockConstruction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.apache.http.HttpHost;
import java.util.Map;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.junit.Test;
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
  public void testGetElasticsearchRestClient() {
    RestClientBuilder builder = RestClient.builder(new HttpHost("foo", 0, "scheme"));
    Map<RestHighLevelClient, List<Object>> constructed = new HashMap<>();
    try (MockedStatic<RestClient> mockedClient = mockStatic(RestClient.class)) {
      mockedClient.when(() -> RestClient.builder(new HttpHost("host", 100, "http"))).thenReturn(builder);
      
      Map<String, Object> m = new HashMap<>();
      m.put("elasticsearch.url", "http://host:100");
      Config config = ConfigFactory.parseMap(m);

      m = new HashMap<>();
      Config nullConfig = ConfigFactory.parseMap(m);
      
      ElasticsearchUtils.getElasticsearchRestClient(config);
      // assertEquals(constructed.get(mockedConstructor.constructed().get(0)).get(0), builder);
      assertThrows(NullPointerException.class, () -> ElasticsearchUtils.getElasticsearchRestClient(nullConfig));
    }

  }
}
