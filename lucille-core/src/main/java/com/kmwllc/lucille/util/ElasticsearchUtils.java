package com.kmwllc.lucille.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Map;
import javax.net.ssl.SSLContext;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient;
import org.apache.hc.client5.http.impl.async.HttpAsyncClients;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.nio.PoolingAsyncClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.nio.ssl.TlsStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;

import java.net.URI;

/**
 * Utility methods for communicating with Elasticsearch.
 */
public class ElasticsearchUtils {

  public static Spec ELASTICSEARCH_PARENT_SPEC = SpecBuilder.parent("elasticsearch")
      .requiredString("index", "url")
      .optionalBoolean("update", "acceptInvalidCert", "useCompression")
      .optionalString("parentName")
      .optionalParent("join", new TypeReference<Map<String, String>>(){}).build();

  public static ElasticsearchClient getElasticsearchOfficialClient(Config config) throws Exception {
    URI hostUri = URI.create(getElasticsearchUrl(config));

    HttpHost host = new HttpHost(hostUri.getScheme(), hostUri.getHost(), hostUri.getPort());

    // get user info from URI if present and setup BasicAuth credentials if needed
    final BasicCredentialsProvider credentialsProvider = new BasicCredentialsProvider();
    String userInfo = hostUri.getUserInfo();
    if (userInfo != null) {
      int pos = userInfo.indexOf(":");
      String username = userInfo.substring(0, pos);
      String password = userInfo.substring(pos + 1);
      credentialsProvider.setCredentials(new AuthScope(host),
          new UsernamePasswordCredentials(username, password.toCharArray()));
    }

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
        .setConnectionManager(PoolingAsyncClientConnectionManagerBuilder.create()
            .setTlsStrategy(tlsStrategy)
            .build())
        .build();

    boolean useCompression = config.hasPath("elasticsearch.useCompression") && config.getBoolean("elasticsearch.useCompression");
    Rest5Client rest5Client = Rest5Client.builder(host)
        .setHttpClient(httpClient)
        .setCompressionEnabled(useCompression)
        .build();

    ElasticsearchTransport transport = new Rest5ClientTransport(rest5Client, new JacksonJsonpMapper());
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
