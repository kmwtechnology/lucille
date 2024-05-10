package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import java.util.HashMap;
import java.util.Map;
import org.junit.Test;
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

  }
}
