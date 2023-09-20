package com.kmwllc.lucille.util;

import com.typesafe.config.Config;
import java.net.URI;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import javax.net.ssl.SSLEngine;
import nl.altindag.ssl.SSLFactory;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManager;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.function.Factory;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.reactor.ssl.TlsDetails;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.transport.OpenSearchTransport;
import org.opensearch.client.transport.httpclient5.ApacheHttpClient5TransportBuilder;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;

/**
 * Utility methods for communicating with OpenSearch.
 */
public class OpenSearchUtils {

  public static OpenSearchClient getOpenSearchRestClient(Config config) {
    return getOpenSearchRestClientInternal(config);
  }

  /**
   * Generate a RestHighLevelClient from the given config file. Supports Http OpenSearchClients.
   *
   * @param config The configuration file to generate a client from
   * @return the RestHighLevelClient client
   */
  public static OpenSearchClient getOpenSearchRestClientInternal(Config config) {
    try {
      var env = System.getenv();
      var https = Boolean.parseBoolean(env.getOrDefault("HTTPS", "true"));
      var hostname = env.getOrDefault("HOST", "localhost");
      var port = Integer.parseInt(env.getOrDefault("PORT", "9200"));
      var user = env.getOrDefault("USERNAME", "admin");
      var pass = env.getOrDefault("PASSWORD", "admin");

      // get host uri
      URI hostUri = URI.create(getOpenSearchUrl(config));

      String userInfo = hostUri.getUserInfo();

      final var hosts = new HttpHost[] {
          new HttpHost(hostUri.getScheme(), hostUri.getHost(), hostUri.getPort())
      };


      final var sslContext = SSLContextBuilder.create()
          .loadTrustMaterial(null, (chains, authType) -> true)
          .build();


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

            // Disable SSL/TLS verification as our local testing clusters use self-signed certificates
            final var tlsStrategy = ClientTlsStrategyBuilder.create()
                .setSslContext(sslContext)
                .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .build();

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

//    //Establish credentials to use basic authentication.
//    final BasicCredentialsProvider provider = new BasicCredentialsProvider();
//
//    // get user info from URI if present and setup BasicAuth credentials if needed
//    String userInfo = hostUri.getUserInfo();
//    HttpHost host = new HttpHost(hostUri.getScheme(), hostUri.getHost(), hostUri.getPort());
//    if (userInfo != null) {
//      int pos = userInfo.indexOf(":");
//      String username = userInfo.substring(0, pos);
//      String password = userInfo.substring(pos + 1);
//      provider.setCredentials(new AuthScope(host),
//          new UsernamePasswordCredentials(username, password.toCharArray()));
//    }
//
//    // needed to allow for local testing of HTTPS
//    SSLFactory.Builder sslFactoryBuilder = SSLFactory.builder();
//    boolean allowInvalidCert = getAllowInvalidCert(config);
//    if (allowInvalidCert) {
//      sslFactoryBuilder
//          .withTrustingAllCertificatesWithoutValidation()
//          .withHostnameVerifier((sampleHost, sampleSession) -> true);
//    } else {
//      sslFactoryBuilder.withDefaultTrustMaterial();
//    }
//
//    SSLFactory sslFactory = sslFactoryBuilder.build();
//
//    ApacheHttpClient5TransportBuilder builder = ApacheHttpClient5TransportBuilder.builder(host);
//    builder.setHttpClientConfigCallback(httpClientBuilder -> {
//      final TlsStrategy tlsStrategy = ClientTlsStrategyBuilder.create()
//          //.setSslContext(SSLContextBuilder.create().build())
//          .setSslContext(sslFactory.getSslContext())
//          .setHostnameVerifier(sslFactory.getHostnameVerifier())
//          .setTlsDetailsFactory(new Factory<SSLEngine, TlsDetails>() {
//            @Override
//            public TlsDetails create(final SSLEngine sslEngine) {
//              return new TlsDetails(sslEngine.getSession(), sslEngine.getApplicationProtocol());
//            }
//  //            @Override
//  //            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//  //              return httpClientBuilder.setDefaultCredentialsProvider(provider).setSSLContext()
//  //                  .setSSLHostnameVerifier(sslFactory.getHostnameVerifier());
//  //            }
//        }).build();
//      final PoolingAsyncClientConnectionManager connectionManager = PoolingAsyncClientConnectionManagerBuilder
//          .create()
//          .setTlsStrategy(tlsStrategy)
//          .build();
//
//      return httpClientBuilder
//          .setDefaultCredentialsProvider(provider)
//          .setConnectionManager(connectionManager);
//    });
//
//    final OpenSearchTransport transport = ApacheHttpClient5TransportBuilder.builder(host).build();
//    final OpenSearchClient client = new OpenSearchClient(transport);
//
//    return client;
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
