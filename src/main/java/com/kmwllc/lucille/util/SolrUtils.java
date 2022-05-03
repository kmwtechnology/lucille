package com.kmwllc.lucille.util;


import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.typesafe.config.Config;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;

import java.util.List;
import java.util.Map;

/**
 * Utility methods for communicating with Solr.
 */
public class SolrUtils {

  private static final String KEYSTORE_PASSWORD = "javax.net.ssl.keyStorePassword";
  private static final String KEYSTORE = "javax.net.ssl.keyStore";
  private static final String TRUSTSTORE = "javax.net.ssl.trustStore";
  private static final String TRUSTSTORE_PASSWORD = "javax.net.ssl.trustStorePassword";

  private static final Logger log = LogManager.getLogger(SolrUtils.class);

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients, and will set SSL properties if specified.
   *
   * @param config The configuration file to generate a client from
   * @return the solr client
   */
  public static SolrClient getSolrClient(Config config) {
    setSSLSystemProperties(config);
    if (config.hasPath("useCloudClient") && config.getBoolean("useCloudClient")) {
      return requiresAuth(config) ? new CloudSolrClient.Builder(getSolrUrls(config)).withHttpClient(getHttpClient(config)).build() : new CloudSolrClient.Builder(getSolrUrls(config)).build();
    } else {
      return requiresAuth(config) ? new HttpSolrClient.Builder(getSolrUrl(config)).withHttpClient(getHttpClient(config)).build() : new HttpSolrClient.Builder(getSolrUrl(config)).build();
    }
  }

  /**
   * Generates a HttpClient with preemptive authentication if required.
   *
   * @param config The configuration file to generate the HttpClient from.
   * @return the HttpClient
   */
  public static HttpClient getHttpClient(Config config) {
    HttpClientBuilder clientBuilder = HttpClientBuilder.create();

    if (requiresAuth(config)) {
      CredentialsProvider provider = new BasicCredentialsProvider();
      String userName = config.getString("solr.userName");
      String password = config.getString("solr.password");
      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(userName, password);
      provider.setCredentials(AuthScope.ANY, credentials);
      clientBuilder.setDefaultCredentialsProvider(provider);
      clientBuilder.addInterceptorFirst(new PreemptiveAuthInterceptor());
    }

    return clientBuilder.build();
  }

  /**
   * Determines whether the connection to Solr requires authentication.
   *
   * @param config
   * @return
   */
  public static boolean requiresAuth(Config config) {
    boolean hasUserName = config.hasPath("solr.userName");
    boolean hasPassword = config.hasPath("solr.password");
    if (hasUserName != hasPassword) {
      log.error("Both the userName and password must be set.");
    }
    return hasUserName && hasPassword;
  }

  public static String getSolrUrl(Config config) {
    return config.getString("solr.url");
  }

  public static List<String> getSolrUrls(Config config) {
    return config.getStringList("solr.url");
  }

  /**
   * This method will convert a Tuple produced by a Solr Streaming Expression into a Lucille Document. The Tuple must
   * have a value in its id field in order to be converted.
   *
   * @param tuple a Tuple from Solr
   * @return a Document
   */
  public static Document toDocument(Tuple tuple) throws DocumentException {
    Document d;
    if (tuple.getString(Document.ID_FIELD) != null) {
      d = new Document(tuple.getString(Document.ID_FIELD));
    } else {
      throw new DocumentException("Unable to create Document from Tuple. No id field present.");
    }

    for (Map.Entry<Object, Object> e : tuple.getFields().entrySet()) {
      d.setOrAdd((String) e.getKey(), (String) e.getValue());
    }

    return d;
  }

  /**
   * Sets SSL system properties if they are specified in the config.
   * The config can read environment variables if lines like the following are included: javax.net.ssl.keyStorePassword = ${?KEYSTORE_PASSWORD}”
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
