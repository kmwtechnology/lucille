package com.kmwllc.lucille.util;

import org.apache.solr.client.solrj.SolrClient;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;

public class SolrUtils {

  public static SolrClient getSolrClient(Config config) {
    if (config.hasPath("useCloudClient") && config.getBoolean("useCloudClient")) {
      return new CloudSolrClient.Builder(config.getStringList("solrUrl")).build();
    } else {
      return new HttpSolrClient.Builder(config.getString("solrUrl")).build();
    }
  }

}
