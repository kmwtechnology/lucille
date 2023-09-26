package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;

/**
 * Utility methods for communicating with OpenSearch.
 */
public class OpenSearchUtils {

  /**
   * Generate a RestHighLevelClient from the given config file. Supports Apache Http OpenSearchClient
   *
   * @param config The configuration file to generate a client from
   * @return the RestHighLevelClient client
   */
  public static OpenSearchClient getOpenSearchRestClient(Config config) {
    try {

      // get host uri
      URI hostUri = URI.create(getOpenSearchUrl(config));

      String userInfo = hostUri.getUserInfo();

      final var hosts = new HttpHost[]{
          new HttpHost(hostUri.getScheme(), hostUri.getHost(), hostUri.getPort())
      };


      // code for Apache Client is taken from following link:
      // https://github.com/opensearch-project/opensearch-java/blob/main/samples/src/main/java/org/opensearch/client/samples/SampleClient.java
      // When comparing to example code, here are differences:
      //  - We gather data from our config rather than providing directly
      //  - We disable TLS/SSL verification only if acceptInvalidCerts is true (from config)
      final var transport = ApacheHttpClient5TransportBuilder
          .builder(hosts)
          .setMapper(new JacksonJsonpMapper())
          .setHttpClientConfigCallback(httpClientBuilder -> {
            final var credentialsProvider = new BasicCredentialsProvider();
            if (userInfo != null) {
              int pos = userInfo.indexOf(":");
              String username = userInfo.substring(0, pos);
              String password = userInfo.substring(pos + 1);
              for (final var host : hosts) {
                credentialsProvider.setCredentials(
                    new AuthScope(host),
                    new UsernamePasswordCredentials(username, password.toCharArray()));
              }
            }

            // Potentially disable SSL/TLS verification for when testing locally
            boolean allowInvalidCert = getAllowInvalidCert(config);
            TlsStrategy tlsStrategy = null;
            SSLContext sslContext = null;
            try {
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
            } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
              throw new RuntimeException(e);
            }

            final var connectionManager = PoolingAsyncClientConnectionManagerBuilder.create()
                .setTlsStrategy(tlsStrategy)
                .build();

            return httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)
                .setConnectionManager(connectionManager);
          })
          .build();

      return new OpenSearchClient(transport);
    } catch (Exception e) {
      System.out.println("failed to make transport client");
      return null;
    }
  }

  public static String getOpenSearchUrl(Config config) {
    return config.getString("opensearch.url"); // not optional, throws exception if not found
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
