package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.http.client.HttpClient;
import org.junit.Test;

import static org.junit.Assert.*;


public class SSLUtilsTest {

  @Test
  public void testSSL() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:SSLUtilsTest/ssl.conf"));

    // clear system properties
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.keyStore");
    System.clearProperty("javax.net.ssl.trustStore");

    // set property before calling setSSLSystemProperties
    System.setProperty("javax.net.ssl.keyStore", "path/to/file");

    SSLUtils.setSSLSystemProperties(config);

    // verify that system property is set via the config
    assertEquals("secret", System.getProperty("javax.net.ssl.keyStorePassword"));

    // verify that system property is set through the terminal (as a system.setProperty)
    assertEquals("path/to/file", System.getProperty("javax.net.ssl.keyStore"));

    // verify that if both the system property is set through the config and the terminal, the terminal takes precedence
    System.setProperty("javax.net.ssl.trustStore", "/more/important/path/to/file");
    SSLUtils.setSSLSystemProperties(config);
    assertEquals("/more/important/path/to/file", System.getProperty("javax.net.ssl.trustStore"));

    // verify that if nothing is set if not specified
    assertNull(System.getProperty("javax.net.ssl.trustStorePassword"));
  }
}
