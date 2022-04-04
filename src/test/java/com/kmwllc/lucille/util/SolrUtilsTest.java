package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.commons.io.IOUtils;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.junit.Test;

import java.io.File;
import java.io.Reader;
import java.net.http.HttpClient;

import static com.kmwllc.lucille.util.SolrUtils.getSolrClient;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SolrUtilsTest {
  @Test
  public void requireAuthTest() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:SolrUtilsTest/auth.conf"));

    assertTrue(SolrUtils.requiresAuth(config));
  }
}
