package com.kmwllc.lucille.solr.util;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.net.ssl.SSLException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.solr.SolrTestCaseJ4;
import org.apache.solr.SolrTestCaseJ4.SuppressSSL;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.CoreAdminRequest;
import org.apache.solr.embedded.JettyConfig;
import org.apache.solr.util.SSLTestConfig;
import org.apache.solr.util.SolrJettyTestRule;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

/**
 * Connects to a standalone Solr serving HTTPS with the test framework's self-signed certificate, which the JVM does
 * not trust, to check that solr.acceptInvalidCert is what lets the connection through.
 */
// SuppressSSL stops the framework from configuring SSL itself, including a client trust store that would trust the cert
@SuppressSSL
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
public class SolrUtilsSslIT extends SolrTestCaseJ4 {

  @ClassRule
  public static final SolrJettyTestRule solrRule = new SolrJettyTestRule();

  @BeforeClass
  public static void startSolr() throws Exception {
    Path solrHome = createTempDir("solrhome");
    // without one, Solr looks for its default solr.xml under tests.src.home, which the pom points at test-classes
    Files.writeString(solrHome.resolve("solr.xml"), "<solr/>");
    JettyConfig jettyConfig = JettyConfig.builder()
        .withSSLConfig(new SSLTestConfig(true, false).buildServerSSLConfig())
        .build();
    solrRule.startSolr(solrHome, new Properties(), jettyConfig);
  }

  private static Config config(boolean acceptInvalidCert) {
    Map<String, Object> map = new HashMap<>();
    map.put("solr.url", List.of(solrRule.getBaseUrl()));
    map.put("solr.acceptInvalidCert", acceptInvalidCert);
    return ConfigFactory.parseMap(map);
  }

  @Test
  public void testAcceptInvalidCertConnects() throws Exception {
    assertTrue(solrRule.getBaseUrl().startsWith("https://"));
    try (SolrClient client = SolrUtils.getSolrClient(config(true))) {
      assertEquals(0, CoreAdminRequest.getStatus(null, client).getStatus());
    }
  }

  @Test
  public void testInvalidCertRejectedByDefault() throws Exception {
    try (SolrClient client = SolrUtils.getSolrClient(config(false))) {
      Exception e = expectThrows(Exception.class, () -> CoreAdminRequest.getStatus(null, client));
      assertTrue("expected a TLS failure, got " + e,
          ExceptionUtils.indexOfType(e, SSLException.class) >= 0);
    }
  }
}
