package com.kmwllc.lucille.util;

import org.apache.solr.client.solrj.SolrClient;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

import java.util.List;

/**
 * Utility methods for communicating with Solr.
 */
public class SolrUtils {

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients.
   *
   * @param config  The configuration file to generate a client from
   * @return  the solr client
   */
  public static SolrClient getSolrClient(Config config) {
    if (config.hasPath("useCloudClient") && config.getBoolean("useCloudClient")) {
      return new CloudSolrClient.Builder(getSolrUrls(config)).build();
    } else {
      return new HttpSolrClient.Builder(getSolrUrl(config)).build();
    }
  }

  public static String getSolrUrl(Config config) {
    if (config.hasPath("solr.url")) {
      return config.getString("solr.url");
    } else {
      return config.getString("solrUrl");
    }
  }

  public static List<String> getSolrUrls(Config config) {
    if (config.hasPath("solr.url")) {
      return config.getStringList("solr.url");
    } else {
      return config.getStringList("solrUrl");
    }
  }

}
