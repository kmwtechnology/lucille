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
import java.util.Optional;

/**
 * Utility methods for communicating with Solr.
 */
public class SolrUtils {

  private static final Logger log = LogManager.getLogger(SolrUtils.class);

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients.
   * <p>
   * This method has SIDE EFFECTS.  It will set SSL system properties if corresponding properties
   * are found in the config.
   *
   * @param config The configuration file to generate a client from
   * @return the solr client
   */
  public static SolrClient getSolrClient(Config config) {
    SSLUtils.setSSLSystemProperties(config);
    if (config.hasPath("solr.useCloudClient") && config.getBoolean("solr.useCloudClient")) {
      CloudSolrClient cloudSolrClient = getCloudClient(config);
      if (config.hasPath("solr.defaultCollection")) {
        cloudSolrClient.setDefaultCollection(config.getString("solr.defaultCollection"));
      }
      return cloudSolrClient;
    } else {
      return requiresAuth(config) ?
        new HttpSolrClient.Builder(getSolrUrl(config)).withHttpClient(getHttpClient(config)).build() :
        new HttpSolrClient.Builder(getSolrUrl(config)).build();
    }
  }

  private static CloudSolrClient getCloudClient(Config config) {
    // todo review if zkHosts is required
    // https://solr.apache.org/guide/solr/latest/deployment-guide/solrj.html#base-urls-of-cloudsolrclient
    // https://solr.apache.org/guide/6_6/setting-up-an-external-zookeeper-ensemble.html
    CloudSolrClient.Builder cloudBuilder;
    if (config.hasPath("solr.zkHosts")) {

      // optional property
      Optional<String> zkChroot;
      if (config.hasPath("solr.zkChroot")) {
        zkChroot = Optional.of(config.getString("solr.zkChroot"));
      } else {
        zkChroot = Optional.empty();
      }

      cloudBuilder = new CloudSolrClient.Builder(config.getStringList("solr.zkHosts"), zkChroot);
    } else {
      cloudBuilder = new CloudSolrClient.Builder(getSolrUrls(config));
    }

    if (requiresAuth(config)) {
      cloudBuilder.withHttpClient(getHttpClient(config));
    }

    return cloudBuilder.build();
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
      d = Document.create(tuple.getString(Document.ID_FIELD));
    } else {
      throw new DocumentException("Unable to create Document from Tuple. No id field present.");
    }

    for (Map.Entry<Object, Object> e : tuple.getFields().entrySet()) {
      d.setOrAdd((String) e.getKey(), (String) e.getValue());
    }

    return d;
  }
}
