package com.kmwllc.lucille.util;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import nl.altindag.ssl.SSLFactory;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.URI;

/**
 * Utility methods for communicating with Elasticsearch.
 */
public class ElasticsearchUtils {

  /**
   * Generate a RestHighLevelClient from the given config file. Supports Http ElasticsearchClients.
   *
   * @param config The configuration file to generate a client from
   * @return the RestHighLevelClient client
   */
  public static RestHighLevelClient getElasticsearchRestClient(Config config) {

    // get host uri
    URI hostUri = URI.create(getElasticsearchUrl(config));

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

    RestClientBuilder builder = RestClient.builder(new HttpHost(hostUri.getHost(), hostUri.getPort(),
            hostUri.getScheme()))
        .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder.setDefaultCredentialsProvider(provider).setSSLContext(sslFactory.getSslContext())
                .setSSLHostnameVerifier(sslFactory.getHostnameVerifier());
          }
        });

    RestHighLevelClient client = new RestHighLevelClient(builder);

    return client;
  }

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

  public static class ElasticJoinData {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private String joinFieldName;
    private boolean isChild;
    private String parentName;
    private String childName;
    private String parentDocumentIdSource;

    public void addJoinData(Document doc) {

      // no need to do a join
      if (joinFieldName == null) {
        return;
      }

      // check that join field doesn't already exist
      if (doc.has(joinFieldName)) {
        throw new IllegalStateException("Document already has join field: " + joinFieldName);
      }

      // set a necessary join field
      if (isChild) {
        String parentId = doc.getString(parentDocumentIdSource);
        doc.setField(joinFieldName, getChildNode(parentId));
      } else {
        doc.setField(joinFieldName, parentName);
      }
    }

    private JsonNode getChildNode(String parentId) {
      return MAPPER.createObjectNode()
          .put("name", childName)
          .put("parent", parentId);
    }

    private static String prefix() {
      return prefix(null);
    }

    private static String prefix(String path) {
      // todo consider abstracting adding prefix to config
      StringBuilder builder = new StringBuilder("elasticsearch.join");
      if (path == null || path.isEmpty()) {
        return builder.toString();
      }
      return builder.append(".").append(path).toString();
    }

    public static ElasticJoinData fromConfig(Config config) {


      ElasticJoinData data = new ElasticJoinData();

      // if no join in config will initialize all join data to null and will be skipped in the code
      if (config.hasPath(prefix())) {

        // set fields for both parent and child
        data.joinFieldName = config.getString(prefix("joinFieldName"));
        data.isChild = ConfigUtils.getOrDefault(config, prefix("isChild"), false);

        // set parent child specific fields
        if (data.isChild) {
          data.childName = config.getString(prefix("childName"));
          data.parentDocumentIdSource = config.getString(prefix("parentDocumentIdSource"));
        } else {
          data.parentName = config.getString(prefix("parentName"));
        }
      }

      return data;
    }
  }
}
