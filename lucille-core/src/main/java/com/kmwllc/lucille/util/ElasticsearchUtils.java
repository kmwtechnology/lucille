package com.kmwllc.lucille.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import com.typesafe.config.Config;
import java.util.Map;
import nl.altindag.ssl.SSLFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;

import java.net.URI;

/**
 * Utility methods for communicating with Elasticsearch.
 */
public class ElasticsearchUtils {

  public static ParentSpec ELASTICSEARCH_PARENT_SPEC = Spec.parent("elasticsearch")
      .reqStr("index", "url")
      .optBool("update", "acceptInvalidCert")
      .optStr("parentName")
      .optParent("join", new TypeReference<Map<String, String>>(){});

  public static ElasticsearchClient getElasticsearchOfficialClient(Config config) {
    URI hostUri = URI.create(getElasticsearchUrl(config));

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
        .setHttpClientConfigCallback(httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(provider)
            .setSSLContext(sslFactory.getSslContext())
            .setSSLHostnameVerifier(sslFactory.getHostnameVerifier())).build();

    ElasticsearchTransport transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
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
