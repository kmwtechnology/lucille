package com.kmwllc.lucille.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import co.elastic.clients.transport.rest5_client.low_level.Rest5ClientBuilder;
import com.typesafe.config.Config;
import java.util.Base64;
import nl.altindag.ssl.SSLFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.message.BasicHeader;

import java.net.URI;

/**
 * Utility methods for communicating with Elasticsearch.
 */
public class ElasticsearchUtils {

  public static ElasticsearchClient getElasticsearchOfficialClient(Config config) {
    URI hostUri = URI.create(getElasticsearchUrl(config));

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

    Rest5ClientBuilder builder = Rest5Client.builder(hostUri)
        .setSSLContext(sslFactory.getSslContext());

    // get user info from URI if present and setup BasicAuth credentials if needed
    String userInfo = hostUri.getUserInfo();
    if (userInfo != null) {
      String creds = Base64.getEncoder().encodeToString(userInfo.getBytes());
      builder.setDefaultHeaders(new Header[] { new BasicHeader("Authorization", "Basic " + creds) });
    }

    /*
    RestClient restClient = RestClient.builder(new HttpHost(hostUri.getHost(), hostUri.getPort(), hostUri.getScheme()))
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(provider)
            .setSSLContext(sslFactory.getSslContext())
            .setSSLHostnameVerifier(sslFactory.getHostnameVerifier())).build();
     */

    Rest5Client client = builder.build();
    ElasticsearchTransport transport = new Rest5ClientTransport(client, new JacksonJsonpMapper());
    return new ElasticsearchClient(transport);
  }

  public static String getElasticsearchUrl(Config config) {
    return config.getString("elasticsearch.url"); // not optional, throws exception if not found
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
