package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.FileContentFetcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SSLUtilsTest {
  private static final String KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  private static final String KEYSTORE = "javax.net.ssl.keyStore";
  private static final String TRUSTSTORE = "javax.net.ssl.trustStore";
  private static final String TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";

  private String originalKeyStore = null;
  private String originalKeyStorePassword = null;
  private String originalTrustStore = null;
  private String originalTrustStorePassword = null;

  @Before
  public void setup() {
    originalKeyStore = System.getProperty(KEYSTORE);
    originalKeyStorePassword = System.getProperty(KEYSTORE_PASSWORD);
    originalTrustStore = System.getProperty(TRUSTSTORE);
    originalTrustStorePassword = System.getProperty(TRUSTSTORE_PASSWORD);
  }

  @After
  public void tearDown() {
    if (originalKeyStore == null) {
      System.clearProperty("javax.net.ssl.keyStore");
    } else {
      System.setProperty("javax.net.ssl.keyStore", originalKeyStore);
    }

    if (originalKeyStorePassword == null) {
      System.clearProperty("javax.net.ssl.keyStorePassword");
    } else {
      System.setProperty("javax.net.ssl.keyStorePassword", originalKeyStorePassword);
    }

    if (originalTrustStore == null) {
      System.clearProperty("javax.net.ssl.trustStore");
    } else {
      System.setProperty("javax.net.ssl.trustStore", originalTrustStore);
    }

    if (originalTrustStorePassword == null) {
      System.clearProperty("javax.net.ssl.trustStorePassword");
    } else {
      System.setProperty("javax.net.ssl.trustStorePassword", originalTrustStorePassword);
    }
  }

  @Test
  public void testSSL() throws Exception {

    // config contains:
    //      javax.net.ssl.keyStore: "/path/to/keyStore",
    //      javax.net.ssl.keyStorePassword: "secret"
    //      javax.net.ssl.trustStore: "/path/to/trustStore"
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:SSLUtilsTest/ssl.conf"));

    System.setProperty("javax.net.ssl.keyStore", "/different/path/to/keyStore");
    System.clearProperty("javax.net.ssl.keyStorePassword");
    System.clearProperty("javax.net.ssl.trustStore");
    System.clearProperty("javax.net.ssl.trustStorePassword");

    assertEquals("/different/path/to/keyStore", System.getProperty("javax.net.ssl.keyStore"));
    assertNull(System.getProperty("javax.net.ssl.keyStorePassword"));
    assertNull(System.getProperty("javax.net.ssl.trustStore"));
    assertNull(System.getProperty("javax.net.ssl.trustStorePassword"));

    SSLUtils.setSSLSystemProperties(config);

    // if a system property was already set, it should not be overridden by the config
    assertEquals("/different/path/to/keyStore", System.getProperty("javax.net.ssl.keyStore"));

    // if a system property was not set, it should now be set according to the config
    assertEquals("secret", System.getProperty("javax.net.ssl.keyStorePassword"));
    assertEquals("/path/to/trustStore", System.getProperty("javax.net.ssl.trustStore"));

    // if a system property was not set and is not present in the config, it should remain unset
    assertNull(System.getProperty("javax.net.ssl.trustStorePassword"));
  }

  @Test
  public void testAllPropertiesInConfigAndNotSet() {
    try {
      Config config = ConfigFactory.parseResourcesAnySyntax("SSLUtilsTest/all.conf");

      SSLUtils.setSSLSystemProperties(config);

      // all four of them should have been set.
      assertEquals("/all/path/keyStore", System.getProperty(KEYSTORE));
      assertEquals("all-secret", System.getProperty(KEYSTORE_PASSWORD));
      assertEquals("/all/path/trustStore", System.getProperty(TRUSTSTORE));
      assertEquals("all-trust-secret", System.getProperty(TRUSTSTORE_PASSWORD));
    } finally {
      System.clearProperty(KEYSTORE);
      System.clearProperty(KEYSTORE_PASSWORD);
      System.clearProperty(TRUSTSTORE);
      System.clearProperty(TRUSTSTORE_PASSWORD);
    }
  }

  @Test
  public void testAllPropertiesInConfigButSet() {
    try {
      System.setProperty(KEYSTORE, "/default/keyStore");
      System.setProperty(KEYSTORE_PASSWORD, "default-key-secret");
      System.setProperty(TRUSTSTORE, "/default/trustStore");
      System.setProperty(TRUSTSTORE_PASSWORD, "default-trust-secret");

      Config config = ConfigFactory.parseResourcesAnySyntax("SSLUtilsTest/all.conf");

      SSLUtils.setSSLSystemProperties(config);

      // none of them should've been overwritten.
      assertEquals("/default/keyStore", System.getProperty(KEYSTORE));
      assertEquals("default-key-secret", System.getProperty(KEYSTORE_PASSWORD));
      assertEquals("/default/trustStore", System.getProperty(TRUSTSTORE));
      assertEquals("default-trust-secret", System.getProperty(TRUSTSTORE_PASSWORD));
    } finally {
      System.clearProperty(KEYSTORE);
      System.clearProperty(KEYSTORE_PASSWORD);
      System.clearProperty(TRUSTSTORE);
      System.clearProperty(TRUSTSTORE_PASSWORD);
    }
  }

  @Test
  public void testAllPropertiesMissingButSet() {
    try {
      System.setProperty(KEYSTORE, "/default/keyStore");
      System.setProperty(KEYSTORE_PASSWORD, "default-key-secret");
      System.setProperty(TRUSTSTORE, "/default/trustStore");
      System.setProperty(TRUSTSTORE_PASSWORD, "default-trust-secret");

      Config config = ConfigFactory.empty();

      SSLUtils.setSSLSystemProperties(config);

      // none of them should've been cleared or overwritten, somehow.
      assertEquals("/default/keyStore", System.getProperty(KEYSTORE));
      assertEquals("default-key-secret", System.getProperty(KEYSTORE_PASSWORD));
      assertEquals("/default/trustStore", System.getProperty(TRUSTSTORE));
      assertEquals("default-trust-secret", System.getProperty(TRUSTSTORE_PASSWORD));
    } finally {
      System.clearProperty(KEYSTORE);
      System.clearProperty(KEYSTORE_PASSWORD);
      System.clearProperty(TRUSTSTORE);
      System.clearProperty(TRUSTSTORE_PASSWORD);
    }
  }
}
