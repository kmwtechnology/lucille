package com.kmwllc.lucille.util;

import com.typesafe.config.Config;

public class SSLUtils {

  private static final String KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  private static final String KEYSTORE = "javax.net.ssl.keyStore";
  private static final String TRUSTSTORE = "javax.net.ssl.trustStore";
  private static final String TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";

  /**
   * Sets SSL system properties if they are specified in the config. Looks for config settings:
   * javax.net.ssl.keyStorePassword, javax.net.ssl.keyStore, javax.net.ssl.trustStore,
   * javax.net.ssl.trustStorePassword and sets system properties with the same name. Does not
   * override system properties that are already set via -D or otherwise. The config can read
   * environment variables if lines like the following are included: javax.net.ssl.keyStorePassword
   * = ${?KEYSTORE_PASSWORD}
   *
   * @param config
   */
  public static void setSSLSystemProperties(Config config) {
    if (config.hasPath(KEYSTORE_PASSWORD) && System.getProperty(KEYSTORE_PASSWORD) == null) {
      System.setProperty(KEYSTORE_PASSWORD, config.getString(KEYSTORE_PASSWORD));
    }
    if (config.hasPath(KEYSTORE) && System.getProperty(KEYSTORE) == null) {
      System.setProperty(KEYSTORE, config.getString(KEYSTORE));
    }
    if (config.hasPath(TRUSTSTORE_PASSWORD) && System.getProperty(TRUSTSTORE_PASSWORD) == null) {
      System.setProperty(TRUSTSTORE_PASSWORD, config.getString(TRUSTSTORE_PASSWORD));
    }
    if (config.hasPath(TRUSTSTORE) && System.getProperty(TRUSTSTORE) == null) {
      System.setProperty(TRUSTSTORE, config.getString(TRUSTSTORE));
    }
  }
}
