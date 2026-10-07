package com.kmwllc.lucille.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

import java.util.List;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.junit.Test;

public class HttpHostUtilsTest {

  @Test
  public void testCredentialsScopedToEachHost() {
    BasicCredentialsProvider provider = new BasicCredentialsProvider();
    HttpHost[] hosts = HttpHostUtils.toHttpHosts(
        List.of("https://alice:secret@node-a:9200", "https://bob:p:w@node-b:9201", "http://node-c:9202"), provider);

    assertEquals(3, hosts.length);
    assertEquals(new HttpHost("https", "node-a", 9200), hosts[0]);
    assertEquals(new HttpHost("https", "node-b", 9201), hosts[1]);
    assertEquals(new HttpHost("http", "node-c", 9202), hosts[2]);

    UsernamePasswordCredentials a = (UsernamePasswordCredentials) provider.getCredentials(new AuthScope(hosts[0]), null);
    assertEquals("alice", a.getUserName());
    assertArrayEquals("secret".toCharArray(), a.getUserPassword());

    // only the first ':' separates username from password
    UsernamePasswordCredentials b = (UsernamePasswordCredentials) provider.getCredentials(new AuthScope(hosts[1]), null);
    assertEquals("bob", b.getUserName());
    assertArrayEquals("p:w".toCharArray(), b.getUserPassword());

    assertNull(provider.getCredentials(new AuthScope(hosts[2]), null));
  }

  @Test
  public void testEmptyListRejected() {
    assertThrows(IllegalArgumentException.class, () -> HttpHostUtils.toHttpHosts(List.of(), new BasicCredentialsProvider()));
  }
}
