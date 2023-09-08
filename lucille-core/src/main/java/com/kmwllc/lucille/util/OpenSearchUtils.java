package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.net.URI;
import nl.altindag.ssl.SSLFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder.HttpClientConfigCallback;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.rest_client.RestClientTransport;

/**
 * Utility methods for communicating with OpenSearch.
 */
public class OpenSearchUtils {

  /**
   * Generate a RestHighLevelClient from the given config file. Supports Http OpenSearchClients.
   *
   * @param config The configuration file to generate a client from
   * @return the RestHighLevelClient client
   */
  public static OpenSearchClient getOpenSearchRestClient(Config config) {

    // get host uri
    URI hostUri = URI.create(getOpenSearchUrl(config));

    //Establish credentials to use basic authentication.
    final CredentialsProvider provider = new BasicCredentialsProvider();

    // get user info from URI if present and setup BasicAuth credentials if needed
    String userInfo = hostUri.getUserInfo();
    if (userInfo != null) {
      int pos = userInfo.indexOf(":");
      String username = userInfo.substring(0, pos);
      String password = userInfo.substring(pos + 1);
      provider.setCredentials(AuthScope.ANY,
          new UsernamePasswordCredentials(username, password));
    }

    // needed to allow for local testing of HTTPS
    SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();
    boolean allowInvalidCert = getAllowInvalidCert(config);
    if (allowInvalidCert) {
      sslFactoryBuilder
          .withTrustingAllCertificatesWithoutValidation()
          .withHostnameVerifier((host, session) -> true);
    } else {
      sslFactoryBuilder.withDefaultTrustMaterial();
    }

    SSLFactory sslFactory = sslFactoryBuilder.build();

    RestClient restClient = RestClient.builder(new HttpHost(hostUri.getHost(), hostUri.getPort(), hostUri.getScheme()))
        .setHttpClientConfigCallback(new HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(provider).setSSLContext(sslFactory.getSslContext())
                .setSSLHostnameVerifier(sslFactory.getHostnameVerifier());
          }
        }).build();

    final OpenSearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    final OpenSearchClient client = new OpenSearchClient(transport);

    return client;
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
