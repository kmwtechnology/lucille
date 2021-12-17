package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import org.apache.solr.client.solrj.SolrClient;
import com.typesafe.config.Config;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.io.Tuple;

import javax.print.Doc;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Utility methods for communicating with Solr.
 */
public class SolrUtils {

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients.
   *
   * @param config The configuration file to generate a client from
   * @return the solr client
   */
  public static SolrClient getSolrClient(Config config) {
    if (config.hasPath("useCloudClient") && config.getBoolean("useCloudClient")) {
      return new CloudSolrClient.Builder(getSolrUrls(config)).build();
    } else {
      return new HttpSolrClient.Builder(getSolrUrl(config)).build();
    }
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
   * @return  a Document
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

}
