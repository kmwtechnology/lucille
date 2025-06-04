package com.kmwllc.lucille.util;


import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.Spec.ParentSpec;
import java.io.IOException;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.impl.CloudHttp2SolrClient;
import org.apache.solr.client.solrj.impl.Http2SolrClient;
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

  public static final ParentSpec SOLR_PARENT_SPEC = Spec.parent("solr")
      .withRequiredProperties("url")
      .withOptionalProperties("useCloudClients", "zkHosts", "zkChroot", "defaultCollection",
          "userName", "password", "acceptInvalidCert")
      .withOptionalProperties(SSLUtils.SSL_CONFIG_OPTIONAL_PROPERTIES);

  private static final Logger log = LoggerFactory.getLogger(SolrUtils.class);

  /**
   * Generate a SolrClient from the given config file. Supports both Cloud and Http SolrClients.
   * <p>
   * This method has SIDE EFFECTS.  It will set SSL system properties if corresponding properties
   * are found in the config.
   *
   * @param config The configuration file to generate a client from
   * @return A SolrClient suitable for the given configuration.
   */
  public static SolrClient getSolrClient(Config config) {
    SSLUtils.setSSLSystemProperties(config);
    
    if (config.hasPath("solr.useCloudClient") && config.getBoolean("solr.useCloudClient")) {
      return getCloudClient(config);
    } else {
      return getHttpClientAndSetCheckPeerName(config);
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
      cloudBuilder = new CloudHttp2SolrClient.Builder(config.getStringList("solr.url"));
    }
    if (config.hasPath("solr.defaultCollection")) {
      cloudBuilder.withDefaultCollection(config.getString("solr.defaultCollection"));
    }

    if (requiresAuth(config)) {
      Http2SolrClient httpClient = getHttpClientAndSetCheckPeerName(config);
      cloudBuilder.withHttpClient(httpClient);

      // When you give a cloud client an HTTPClient, and the cloudClient is then closed, it *will not* close the
      // httpClient automatically - since it may (should) be a shared resource. Creating an anonymous subclass
      // for this case allows us to gracefully make sure the httpClient does get closed, preventing a resource leak.
      return new CloudHttp2SolrClient(cloudBuilder) {
        @Override
        public void close() throws IOException {
          super.close();
          httpClient.close();
        }
      };
    } else {
      return cloudBuilder.build();
    }
  }

  /**
   * Generates a HttpClient with preemptive authentication if required.
   * This method has SIDE EFFECTS. It will set SSL system properties if corresponding properties
   * are found in the config.
   *
   * @param config The configuration file to generate the HttpClient from.
   * @return the HttpClient
   */
  static Http2SolrClient getHttpClientAndSetCheckPeerName(Config config) {
    Http2SolrClient.Builder clientBuilder = new Http2SolrClient.Builder();

    if (getAllowInvalidCert(config)) {
    	// disable the solr ssl host checking if configured to do so.
    	System.setProperty("solr.ssl.checkPeerName", "false");
    }
    
    if (requiresAuth(config)) {
      String userName = config.getString("solr.userName");
      String password = config.getString("solr.password");
      clientBuilder.withBasicAuthCredentials(userName, password);
    }
    
    return clientBuilder.build();
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
   * @throws DocumentException if tuple does not have a key called Document.ID_FIELD
   * @return a Document whose id is the stringified version of the contents of Document.ID_FIELD in the tuple.
   */
  public static Document toDocument(Tuple tuple) throws DocumentException {
    Document d;

    if (tuple.getFields().containsKey(Document.ID_FIELD)) {
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
