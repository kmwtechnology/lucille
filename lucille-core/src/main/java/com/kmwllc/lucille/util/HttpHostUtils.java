package com.kmwllc.lucille.util;

import java.net.URI;
import java.util.List;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;

/**
 * Shared parsing of search-engine endpoint URLs into Apache HttpClient 5 hosts.
 */
class HttpHostUtils {

  private HttpHostUtils() {
  }

  /**
   * Converts each URL into an HttpHost. Credentials embedded in a URL (<code>https://user:pass@host:9200</code>) are
   * registered with the provider, scoped to that URL's host, so each host authenticates with the credentials given
   * in its own URL.
   *
   * @param urls one or more endpoint URLs
   * @param credentialsProvider receives the credentials found in the URLs
   * @return the hosts, in the order given
   */
  static HttpHost[] toHttpHosts(List<String> urls, BasicCredentialsProvider credentialsProvider) {
    if (urls.isEmpty()) {
      throw new IllegalArgumentException("At least one URL is required.");
    }

    HttpHost[] hosts = new HttpHost[urls.size()];

    for (int i = 0; i < urls.size(); i++) {
      URI hostUri = URI.create(urls.get(i));
      HttpHost host = new HttpHost(hostUri.getScheme(), hostUri.getHost(), hostUri.getPort());
      hosts[i] = host;

      String userInfo = hostUri.getUserInfo();
      if (userInfo != null) {
        int pos = userInfo.indexOf(":");
        String username = userInfo.substring(0, pos);
        String password = userInfo.substring(pos + 1);
        credentialsProvider.setCredentials(new AuthScope(host),
            new UsernamePasswordCredentials(username, password.toCharArray()));
      }
    }

    return hosts;
  }
}
