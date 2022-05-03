package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.junit.Test;

import static org.junit.Assert.*;


public class SolrUtilsTest {
  @Test
  public void requireAuthTest() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/auth.conf"));
    assertTrue(SolrUtils.requiresAuth(config));
  }


  @Test
  public void getHttpClientTest() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/auth.conf"));
    HttpClient client = SolrUtils.getHttpClient(config);
    // would like to inspect the solr client to confirm credentials are configured, but can’t do that so just checking it’s non-null
    assertNotNull(client);
  }

  @Test
  public void testSSL() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/ssl.conf"));

    // clear system properties
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.trustStore");

    // set property before calling setSSLSystemProperties
    System.setProperty("javax.net.ssl.keyStore", "path/to/file");

    SolrUtils.setSSLSystemProperties(config);

    // verify that system property is set via the config
    assertEquals("secret", System.getProperty("javax.net.ssl.keyStorePassword"));

    // verify that system property is set through the terminal (as a system.setProperty)
    assertEquals("path/to/file", System.getProperty("javax.net.ssl.keyStore"));

    // verify that if both the system property is set through the config and the terminal, the terminal takes precedence
    System.setProperty("javax.net.ssl.trustStore", "/more/important/path/to/file");
    SolrUtils.setSSLSystemProperties(config);
    assertEquals("/more/important/path/to/file", System.getProperty("javax.net.ssl.trustStore"));

    // verify that if nothing is set if not specified
    assertNull(System.getProperty("javax.net.ssl.trustStorePassword"));
  }
}
