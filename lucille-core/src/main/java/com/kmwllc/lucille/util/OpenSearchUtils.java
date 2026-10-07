package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.indexer.OpenSearchIndexer;
import com.typesafe.config.Config;
import java.util.List;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for communicating with OpenSearch.
 */
public class OpenSearchUtils {

  public static final Spec OPENSEARCH_PARENT_SPEC = SpecBuilder.parent("opensearch")
      .requiredStringOrList("url")
      .requiredString("index")
      .optionalBoolean("acceptInvalidCert", "useCompression")
      .optionalNumber("connectTimeoutMs", "socketTimeoutMs", "connectionTimeToLiveMs").build();

  private static final Logger log = LoggerFactory.getLogger(OpenSearchUtils.class);

  /**
   * Generate a RestHighLevelClient from the given config file. Supports Apache Http OpenSearchClient
   *
   * @param config The configuration file to generate a client from
   * @return the RestHighLevelClient client
   */
  public static OpenSearchClient getOpenSearchRestClient(Config config) throws Exception {
    // code for building an Apache Client is inspired by the following link:
    // https://github.com/opensearch-project/opensearch-java/blob/main/samples/src/main/java/org/opensearch/client/samples/SampleClient.java
    // When comparing to example code, here are differences:
    //  - We gather data from our config rather than providing directly
    //  - We disable TLS/SSL verification only if acceptInvalidCerts is true (from config)

    // the transport round-robins requests across the hosts and fails over between them
    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    final HttpHost[] hosts = HttpHostUtils.toHttpHosts(getOpenSearchUrls(config), credentialsProvider);

    // Potentially disable SSL/TLS verification for when testing locally
    TlsStrategy tlsStrategy = HttpClientConfigUtils.buildTlsStrategy(getAllowInvalidCert(config));

    boolean useCompression = config.hasPath("opensearch.useCompression") && config.getBoolean("opensearch.useCompression");
    final var transport = ApacheHttpClient5TransportBuilder
        .builder(hosts)
        .setMapper(new JacksonJsonpMapper())
        .setHttpClientConfigCallback(httpClientBuilder -> {
          final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
              .setTlsStrategy(tlsStrategy)
              .setDefaultConnectionConfig(HttpClientConfigUtils.buildConnectionConfig(config, "opensearch"))
              .build();

          return httpClientBuilder
              .setDefaultCredentialsProvider(credentialsProvider)
              .setConnectionManager(connectionManager);
        })
        .setCompressionEnabled(useCompression)
        .build();

    return new OpenSearchClient(transport);
  }

  /**
   * @return the first configured OpenSearch URL. Use {@link #getOpenSearchUrls(Config)} when several are configured.
   */
  public static String getOpenSearchUrl(Config config) {
    return getOpenSearchUrls(config).get(0);
  }

  /**
   * @return the configured OpenSearch URLs; <code>opensearch.url</code> may be a single String or a list of Strings.
   */
  public static List<String> getOpenSearchUrls(Config config) {
    return ConfigUtils.getStringOrList(config, "opensearch.url"); // not optional, throws exception if not found
  }

  public static String getOpenSearchIndex(Config config) {
    return config.getString("opensearch.index"); // not optional, throws exception if not found
  }

  public static boolean getAllowInvalidCert(Config config) {
    if (config.hasPath("opensearch.acceptInvalidCert")) {
      return config.getString("opensearch.acceptInvalidCert").equalsIgnoreCase("true");
    }
    return false;
  }
}
