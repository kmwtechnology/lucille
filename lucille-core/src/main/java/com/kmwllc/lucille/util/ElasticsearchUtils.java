package com.kmwllc.lucille.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.List;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;

/**
 * Utility methods for communicating with Elasticsearch.
 */
public class ElasticsearchUtils {

  public static Spec ELASTICSEARCH_PARENT_SPEC = SpecBuilder.parent("elasticsearch")
      .requiredString("index")
      .requiredStringOrList("url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression")
      .optionalString("parentName")
      .optionalNumber("maxConnectionsPerRoute", "maxConnectionsTotal")
      .optionalParent("join", new TypeReference<Map<String, String>>(){}).build();

  public static ElasticsearchClient getElasticsearchOfficialClient(Config config) throws Exception {
    // get user info from each URL if present and setup BasicAuth credentials if needed
    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    final HttpHost[] hosts = HttpHostUtils.toHttpHosts(getElasticsearchUrls(config), credentialsProvider);

    // Potentially disable SSL/TLS verification for when testing locally
    boolean allowInvalidCert = getAllowInvalidCert(config);
    SSLContext sslContext;
    TlsStrategy tlsStrategy;

    if (allowInvalidCert) {
      sslContext = SSLContextBuilder.create()
          .loadTrustMaterial(null, (chains, authType) -> true)
          .build();
      tlsStrategy = ClientTlsStrategyBuilder.create()
          .setSslContext(sslContext)
          .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
          .build();
    } else {
      sslContext = SSLContextBuilder.create()
          .build();
      tlsStrategy = ClientTlsStrategyBuilder.create()
          .setSslContext(sslContext)
          .build();
    }

    // elasticsearch-java 9.x uses the Apache HttpClient 5 based Rest5Client transport; the legacy
    // org.elasticsearch.client.RestClient (HttpClient 4) is no longer bundled.
    CloseableHttpAsyncClient httpClient = HttpAsyncClients.custom()
        .setDefaultCredentialsProvider(credentialsProvider)
        .setConnectionManager(AsyncConnectionPoolUtils.buildConnectionManager(config, "elasticsearch", tlsStrategy))
        .build();

    boolean useCompression = config.hasPath("elasticsearch.useCompression") && config.getBoolean("elasticsearch.useCompression");
    Rest5Client rest5Client = Rest5Client.builder(hosts)
        .setHttpClient(httpClient)
        .setCompressionEnabled(useCompression)
        .build();

    ElasticsearchTransport transport = new Rest5ClientTransport(rest5Client, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  /**
   * @return the first configured Elasticsearch URL. Use {@link #getElasticsearchUrls(Config)} when several are configured.
   */
  public static String getElasticsearchUrl(Config config) {
    return getElasticsearchUrls(config).get(0);
  }

  /**
   * @return the configured Elasticsearch URLs; <code>elasticsearch.url</code> may be a single String or a list of Strings.
   */
  public static List<String> getElasticsearchUrls(Config config) {
    return ConfigUtils.getStringOrList(config, "elasticsearch.url"); // not optional, throws exception if not found
  }

  public static String getElasticsearchIndex(Config config) {
    return config.getString("elasticsearch.index"); // not optional, throws exception if not found
  }

  public static boolean getAllowInvalidCert(Config config) {
    if (config.hasPath("elasticsearch.acceptInvalidCert")) {
      return config.getString("elasticsearch.acceptInvalidCert").equalsIgnoreCase("true");
    }
    return false;
  }
}
