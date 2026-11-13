package com.kmwllc.lucille.salesforce.connector;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.salesforce.connector.SalesforceClient.FieldSummary;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.SObjectSummary;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient.BinaryBody;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SalesforceRestClientTest {

  private HttpServer server;
  private String base;
  private Handler handler;

  @FunctionalInterface
  private interface Handler {
    Reply handle(HttpExchange exchange) throws IOException;
  }

  private record Reply(int status, byte[] body, String contentType, Map<String, String> headers) {
    private Reply(int status, String body) {
      this(status, body.getBytes(StandardCharsets.UTF_8), "application/json", Map.of());
    }
  }

  @Before
  public void setUp() throws Exception {
    server = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
    server.createContext("/", exchange -> {
      try {
        Reply reply = handler.handle(exchange);
        send(exchange, reply);
      } catch (Exception e) {
        String message = e.getMessage() == null ? "" : e.getMessage();
        send(exchange, 500, message);
      }
    });
    server.start();
    base = "http://localhost:" + server.getAddress().getPort();
  }

  @After
  public void tearDown() {
    server.stop(0);
  }

  @Test
  public void testPasswordGrantThenListsObjects() throws Exception {
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    handler = exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/services/oauth2/token")) {
        bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
        return new Reply(200, "{\"access_token\":\"abc\",\"instance_url\":\"" + base + "/\"}");
      }
      authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
      return new Reply(200, """
          {"sobjects":[{"name":"Account","label":"Account","custom":false,"queryable":true}]}
          """);
    };

    SalesforceRestClient client = new SalesforceRestClient(base, "v67.0", "cid", "secret", "user@example.com",
        "p@ss word+tok", null, null, 2000, false, 0, 5000);
    try {
      client.connect();
      List<SObjectSummary> objects = client.listSObjects();
      assertEquals(base, client.instanceUrl());
      assertEquals(1, objects.size());
      assertEquals("Account", objects.get(0).name());
      assertFalse(objects.get(0).custom());
      assertTrue(objects.get(0).queryable());
    } finally {
      client.close();
    }

    String form = bodies.get(0);
    assertTrue(form.contains("grant_type=password"));
    assertTrue(form.contains("client_id=cid"));
    assertTrue(form.contains("username=user%40example.com"));
    assertTrue(form.contains("password=p%40ss+word%2Btok"));
    assertEquals(List.of("Bearer abc"), authorizations);
  }

  @Test
  public void testClientCredentialsGrant() throws Exception {
    List<String> bodies = Collections.synchronizedList(new ArrayList<>());
    handler = exchange -> {
      bodies.add(new String(exchange.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
      return new Reply(200, "{\"access_token\":\"abc\",\"instance_url\":\"" + base + "\"}");
    };
    SalesforceRestClient client = new SalesforceRestClient(base, "v67.0", "cid", "secret", null, null, null, null,
        2000, false, 0, 5000);
    try {
      client.connect();
    } finally {
      client.close();
    }
    assertTrue(bodies.get(0).contains("grant_type=client_credentials"));
    assertFalse(bodies.get(0).contains("password="));
  }

  @Test
  public void testConfiguredTokenSkipsLogin() throws Exception {
    AtomicInteger tokenCalls = new AtomicInteger();
    handler = exchange -> {
      if (exchange.getRequestURI().getPath().contains("/oauth2/token")) {
        tokenCalls.incrementAndGet();
      }
      return new Reply(200, "{\"sobjects\":[]}");
    };
    SalesforceRestClient client = tokenClient(0);
    try {
      client.connect();
      assertTrue(client.listSObjects().isEmpty());
    } finally {
      client.close();
    }
    assertEquals(0, tokenCalls.get());
  }

  @Test
  public void testInvalidGrantIsReported() throws Exception {
    handler = exchange -> new Reply(400, "{\"error\":\"invalid_grant\",\"error_description\":\"authentication failure\"}");
    SalesforceRestClient client = new SalesforceRestClient(base, "v67.0", "cid", "secret", null, null, null, null,
        2000, false, 0, 5000);
    try {
      IOException error = assertThrows(IOException.class, client::connect);
      assertTrue(error.getMessage().contains("authentication failure"));
    } finally {
      client.close();
    }
  }

  @Test
  public void testQueryErrorIsNotRetried() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    handler = exchange -> {
      calls.incrementAndGet();
      return new Reply(400, "[{\"message\":\"No such column 'Nope'\",\"errorCode\":\"INVALID_FIELD\"}]");
    };
    SalesforceRestClient client = tokenClient(2);
    try {
      client.connect();
      IOException error = assertThrows(IOException.class, () -> client.query("SELECT Nope FROM Account"));
      assertTrue(error.getMessage().contains("INVALID_FIELD"));
      assertTrue(error.getMessage().contains("No such column 'Nope'"));
    } finally {
      client.close();
    }
    assertEquals(1, calls.get());
  }

  @Test
  public void testRetryAfterTooManyRequests() throws Exception {
    AtomicInteger calls = new AtomicInteger();
    handler = exchange -> {
      if (calls.incrementAndGet() == 1) {
        exchange.getResponseHeaders().set("Retry-After", "0");
        return new Reply(429, "{\"message\":\"slow down\"}");
      }
      return new Reply(200, "{\"sobjects\":[]}");
    };
    SalesforceRestClient client = tokenClient(1);
    try {
      client.connect();
      assertTrue(client.listSObjects().isEmpty());
    } finally {
      client.close();
    }
    assertEquals(2, calls.get());
  }

  @Test
  public void testExpiredSessionIsRefreshed() throws Exception {
    AtomicInteger tokenCalls = new AtomicInteger();
    AtomicInteger listCalls = new AtomicInteger();
    List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    handler = exchange -> {
      if (exchange.getRequestURI().getPath().endsWith("/services/oauth2/token")) {
        int n = tokenCalls.incrementAndGet();
        return new Reply(200, "{\"access_token\":\"t" + n + "\",\"instance_url\":\"" + base + "\"}");
      }
      authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
      if (listCalls.incrementAndGet() == 1) {
        return new Reply(401, "[{\"message\":\"Session expired\",\"errorCode\":\"INVALID_SESSION_ID\"}]");
      }
      return new Reply(200, "{\"sobjects\":[]}");
    };
    SalesforceRestClient client = new SalesforceRestClient(base, "v67.0", "cid", "secret", "user@example.com",
        "secret", null, null, 2000, false, 0, 5000);
    try {
      client.connect();
      assertTrue(client.listSObjects().isEmpty());
    } finally {
      client.close();
    }
    assertEquals(2, tokenCalls.get());
    assertEquals(List.of("Bearer t1", "Bearer t2"), authorizations);
  }

  @Test
  public void testDescribeQueryAndQueryMore() throws Exception {
    handler = exchange -> {
      String path = exchange.getRequestURI().getPath();
      if (path.endsWith("/describe")) {
        return new Reply(200, """
            {"fields":[
              {"name":"Id","type":"id"},
              {"name":"Name","type":"string"},
              {"name":"Old__c","type":"string","deprecatedAndHidden":true}
            ]}
            """);
      }
      if (path.endsWith("/queryAll")) {
        String batch = exchange.getRequestHeaders().getFirst("Sforce-Query-Options");
        if (!"batchSize=500".equals(batch)) {
          return new Reply(400, "{\"message\":\"missing batch size\"}");
        }
        return new Reply(200, """
            {"totalSize":2,"done":false,"nextRecordsUrl":"/services/data/v67.0/query/01g-next",
             "records":[{"Id":"001000000000001AAA","Name":"Acme"}]}
            """);
      }
      if (path.endsWith("/query/01g-next")) {
        if (exchange.getRequestHeaders().getFirst("Sforce-Query-Options") != null) {
          return new Reply(400, "{\"message\":\"queryMore should not set a batch size\"}");
        }
        return new Reply(200, """
            {"totalSize":2,"done":true,"records":[{"Id":"001000000000002AAA","Name":"Beta"}]}
            """);
      }
      return new Reply(404, "{\"message\":\"not found\"}");
    };

    SalesforceRestClient client = new SalesforceRestClient(base, "v67.0", null, null, null, null, "token-1", base,
        500, true, 0, 5000);
    try {
      client.connect();
      List<FieldSummary> fields = client.describeFields("Account");
      assertEquals("Id", fields.get(0).name());
      assertEquals("string", fields.get(1).type());
      assertTrue(fields.get(2).deprecatedAndHidden());
      SalesforceClient.QueryPage first = client.query("SELECT Id, Name FROM Account");
      assertFalse(first.done());
      assertEquals("Acme", first.records().get(0).get("Name").asText());
      SalesforceClient.QueryPage second = client.queryMore(first.nextRecordsUrl());
      assertTrue(second.done());
      assertEquals("Beta", second.records().get(0).get("Name").asText());
    } finally {
      client.close();
    }
  }

  @Test
  public void testDownloadFollowsRedirectAndKeepsAuthorization() throws Exception {
    List<String> authorizations = Collections.synchronizedList(new ArrayList<>());
    List<String> paths = Collections.synchronizedList(new ArrayList<>());
    handler = exchange -> {
      authorizations.add(exchange.getRequestHeaders().getFirst("Authorization"));
      String path = exchange.getRequestURI().getPath();
      paths.add(path);
      if (path.endsWith("/VersionData")) {
        return new Reply(302, new byte[0], null, Map.of("Location", base + "/sfc/servlet/version/download"));
      }
      return new Reply(200, "word-bytes".getBytes(StandardCharsets.UTF_8),
          "application/vnd.openxmlformats-officedocument.wordprocessingml.document; charset=UTF-8", Map.of());
    };

    SalesforceRestClient client = tokenClient(0);
    try {
      client.connect();
      BinaryBody body = client.downloadBlob("ContentVersion", "068000000000001AAA", "VersionData", 1000);
      assertArrayEquals("word-bytes".getBytes(StandardCharsets.UTF_8), body.bytes());
      assertEquals("application/vnd.openxmlformats-officedocument.wordprocessingml.document", body.contentType());
    } finally {
      client.close();
    }
    assertEquals(List.of("Bearer token-1", "Bearer token-1"), authorizations);
    assertTrue(paths.get(0).endsWith("/sobjects/ContentVersion/068000000000001AAA/VersionData"));
    assertEquals("/sfc/servlet/version/download", paths.get(1));
  }

  @Test
  public void testDownloadRejectsFileOverMaxBytes() throws Exception {
    handler = exchange -> new Reply(200, "0123456789abcdef");
    SalesforceRestClient client = tokenClient(0);
    try {
      client.connect();
      IOException error = assertThrows(IOException.class,
          () -> client.downloadBlob("Attachment", "00P000000000001AAA", "Body", 10));
      assertTrue(error.getMessage().contains("maxBytes"));
    } finally {
      client.close();
    }
  }

  private SalesforceRestClient tokenClient(int maxRetries) {
    return new SalesforceRestClient(base, "v67.0", null, null, null, null, "token-1", base, 2000, false, maxRetries, 5000);
  }

  private static void send(HttpExchange exchange, int status, String body) throws IOException {
    send(exchange, new Reply(status, body));
  }

  private static void send(HttpExchange exchange, Reply reply) throws IOException {
    byte[] bytes = reply.body() == null ? new byte[0] : reply.body();
    if (reply.contentType() != null) {
      exchange.getResponseHeaders().set("Content-Type", reply.contentType());
    }
    reply.headers().forEach((name, value) -> exchange.getResponseHeaders().set(name, value));
    exchange.sendResponseHeaders(reply.status(), bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }
}
