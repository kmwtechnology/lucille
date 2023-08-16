package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class SolrUtilsTest {
  @Test
  public void requireAuthTest() throws Exception {
    Config config =
        ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/auth.conf"));
    assertTrue(SolrUtils.requiresAuth(config));
  }

  @Test
  public void getHttpClientTest() throws Exception {
    Config config =
        ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/auth.conf"));
    HttpClient client = SolrUtils.getHttpClient(config);
    // would like to inspect the solr client to confirm credentials are configured, but can’t do
    // that so just checking it’s non-null
    assertNotNull(client);
  }
}
