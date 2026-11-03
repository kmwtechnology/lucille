package com.kmwllc.lucille.solr.util;


import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.SSLUtils;
import java.io.IOException;
import java.net.Socket;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509ExtendedTrustManager;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.HttpJdkSolrClient;
import org.apache.solr.client.solrj.io.Tuple;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility methods for communicating with Solr.
 */
public class SolrUtils {

  // url is required for the standalone client and for the cloud client without zkHosts; getSolrClient enforces that
  public static final Spec SOLR_PARENT_SPEC = SpecBuilder.parent("solr")
      .optionalStringOrList("url")
      .optionalString("zkChroot", "defaultCollection", "userName", "password")
      .optionalBoolean("useCloudClient", "acceptInvalidCert")
      .optionalString(SSLUtils.SSL_CONFIG_OPTIONAL_PROPERTIES)
      .optionalList("zkHosts", new TypeReference<List<String>>(){}).build();

  private static final Logger log = LoggerFactory.getLogger(SolrUtils.class);

  private static final X509ExtendedTrustManager TRUST_ALL = new X509ExtendedTrustManager() {
    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType) {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, Socket socket) {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, Socket socket) {
    }

    @Override
    public void checkClientTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
    }

    @Override
    public void checkServerTrusted(X509Certificate[] chain, String authType, SSLEngine engine) {
    }

    @Override
    public X509Certificate[] getAcceptedIssuers() {
      return new X509Certificate[0];
    }
  };

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients.
   * <p>
   * This method has SIDE EFFECTS: it sets the javax.net.ssl.* system properties (key store, trust store and their
   * passwords) found in the config, unless they are already set.
   *
   * @param config The configuration file to generate a client from
   * @return A SolrClient suitable for the given configuration.
   */
  public static SolrClient getSolrClient(Config config) {
    // the SSL keys may be given at the root of the config or inside the solr block (the Solr Specs declare them there)
    SSLUtils.setSSLSystemProperties(config);
    if (config.hasPath("solr")) {
      SSLUtils.setSSLSystemProperties(config.getConfig("solr"));
    }

    boolean cloud = config.hasPath("solr.useCloudClient") && config.getBoolean("solr.useCloudClient");
    if (!config.hasPath("solr.url") && !(cloud && config.hasPath("solr.zkHosts"))) {
      throw new IllegalArgumentException("solr.url is required (or solr.zkHosts with solr.useCloudClient: true).");
    }

    if (cloud) {
      return getCloudClient(config);
    } else {
      return getStandaloneClient(config);
    }
  }

  private static CloudHttp2SolrClient getCloudClient(Config config) {
    CloudHttp2SolrClient.Builder cloudBuilder;
    if (config.hasPath("solr.zkHosts")) {
      // optional property
      Optional<String> zkChroot;
      if (config.hasPath("solr.zkChroot")) {
        zkChroot = Optional.of(config.getString("solr.zkChroot"));
      } else {
        zkChroot = Optional.empty();
      }

      cloudBuilder = new CloudHttp2SolrClient.Builder(config.getStringList("solr.zkHosts"), zkChroot);
    } else {
      cloudBuilder = new CloudHttp2SolrClient.Builder(ConfigUtils.getStringOrList(config, "solr.url"));
    }
    if (config.hasPath("solr.defaultCollection")) {
      cloudBuilder.withDefaultCollection(config.getString("solr.defaultCollection"));
    }

    // Always back CloudHttp2SolrClient with HttpJdkSolrClient (Java's built-in HTTP client).
    // Jetty's default HTTP/2 client throws ClosedChannelException when the server sends GOAWAY
    // while a request is in flight; HttpJdkSolrClient handles that case transparently. It is given no base URL, as the
    // cloud client chooses the node for each request.
    HttpJdkSolrClient httpClient = getHttpClientBuilder(config).build();
    cloudBuilder.withHttpClient(httpClient);

    // CloudHttp2SolrClient does not close a caller-supplied httpClient, so wrap it to prevent leaks.
    return new CloudHttp2SolrClient(cloudBuilder) {
      @Override
      public void close() {
        super.close();
        try {
          if (httpClient != null) {
            httpClient.close();
          }
        } catch (IOException e) {
          log.warn("Error closing HttpJdkSolrClient", e);
        }
      }
    };
  }

  /**
   * Builds the standalone client, which sends every request to the first solr.url and to solr.defaultCollection
   * when a request names no collection.
   */
  static HttpJdkSolrClient getStandaloneClient(Config config) {
    List<String> urls = ConfigUtils.getStringOrList(config, "solr.url");
    if (urls.size() > 1) {
      throw new IllegalArgumentException("solr.url has " + urls.size() + " URLs, but without useCloudClient only one "
          + "can be used. Give a single URL, or set useCloudClient: true.");
    }
    HttpJdkSolrClient.Builder clientBuilder = getHttpClientBuilder(config).withBaseSolrUrl(urls.get(0));
    if (config.hasPath("solr.defaultCollection")) {
      clientBuilder.withDefaultCollection(config.getString("solr.defaultCollection"));
    }
    return clientBuilder.build();
  }

  /**
   * Generates a HttpClient builder with preemptive authentication if required, and one that trusts any certificate
   * and does not check the host name if solr.acceptInvalidCert is true.
   *
   * @param config The configuration file to generate the HttpClient from.
   * @return the HttpClient builder
   */
  static HttpJdkSolrClient.Builder getHttpClientBuilder(Config config) {
    HttpJdkSolrClient.Builder clientBuilder = new HttpJdkSolrClient.Builder();

    if (getAllowInvalidCert(config)) {
      // the trust-all SSLContext has no key managers, so a configured client certificate is not sent
      if (System.getProperty("javax.net.ssl.keyStore") != null) {
        log.warn("solr.acceptInvalidCert is true, so the client certificate in javax.net.ssl.keyStore will not be sent. "
            + "If Solr requires client certificates, use javax.net.ssl.trustStore instead of acceptInvalidCert.");
      }
      clientBuilder.withSSLContext(buildTrustAllSSLContext());
    }

    if (requiresAuth(config)) {
      String userName = config.getString("solr.userName");
      String password = config.getString("solr.password");
      clientBuilder.withBasicAuthCredentials(userName, password);
    }

    return clientBuilder;
  }

  /**
   * Builds an SSLContext that trusts any certificate and skips the host name check. The trust manager has to be an
   * X509ExtendedTrustManager: JSSE wraps a plain X509TrustManager in one that still checks the host name, and
   * java.net.http.HttpClient, which backs HttpJdkSolrClient, otherwise only skips that check through a JVM-wide
   * system property.
   */
  static SSLContext buildTrustAllSSLContext() {
    try {
      SSLContext sslContext = SSLContext.getInstance("TLS");
      sslContext.init(null, new TrustManager[] {TRUST_ALL}, null);
      return sslContext;
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Unable to build an SSLContext that accepts invalid certificates.", e);
    }
  }

  /**
   * Determines whether the connection to Solr requires authentication.
   *
   * @param config The configuration file to check for authentication properties.
   * @return true if authentication is required, false otherwise.
   */
  static boolean requiresAuth(Config config) {
    boolean hasUserName = config.hasPath("solr.userName");
    boolean hasPassword = config.hasPath("solr.password");
    if (hasUserName != hasPassword) {
      log.error("Both the userName and password must be set.");
    }
    return hasUserName && hasPassword;
  }

  /**
   * Converts a Tuple produced by a Solr Streaming Expression into a Lucille Document. The Tuple must
   * have a value in its id field in order to be converted. The types of values are maintained in the conversion. Empty list 
   * are not included in the created document. Array and List types become multivalued fields.
   *
   * @param tuple a Tuple from Solr
   * @throws DocumentException if tuple does not have a key called Document.ID_FIELD, or its value is null or blank
   * @return a Document whose id is the stringified version of the contents of Document.ID_FIELD in the tuple.
   */
  public static Document toDocument(Tuple tuple) throws DocumentException {
    Document d;

    if (tuple.getFields().containsKey(Document.ID_FIELD)) {
      // test the raw value: Tuple.getString() turns a null value into the text "null", which would become the id
      Object rawId = tuple.getFields().get(Document.ID_FIELD);
      if (rawId == null || !Document.isValidId(rawId.toString())) {
        // the id comes from data, so a bad one is a DocumentException rather than the factory's IllegalArgumentException
        throw new DocumentException("Unable to create Document from Tuple. The id is null or blank.");
      }
      d = Document.create(tuple.getString(Document.ID_FIELD));
    } else {
      throw new DocumentException("Unable to create Document from Tuple. No id field present.");
    }

    for (Entry<String, Object> e : tuple.getFields().entrySet()) {
      String key = e.getKey();

      if (key.equals(Document.ID_FIELD)) {
        continue;
      }

      Object val = e.getValue();
      try {
        // check if value is a byte array so that bytes are not individually added as that is not supported
        if (val instanceof byte[]) {
          d.setOrAdd(key, val);
        } else if (val instanceof Object[]) {
          d.update(key, UpdateMode.APPEND, (Object[]) val);
        } else if (val instanceof List) {
          ((List<Object>) val).stream().forEach(v -> d.setOrAdd(key, v));
        } else {
          d.setOrAdd(key, val);
        }
      } catch (IllegalArgumentException exception) {
        throw new DocumentException(exception.getMessage());
      }
    }

    return d;
  }
  
  public static boolean getAllowInvalidCert(Config config) {
    if (config.hasPath("solr.acceptInvalidCert")) {
      return config.getString("solr.acceptInvalidCert").equalsIgnoreCase("true");
    }
    return false;
  }
}
