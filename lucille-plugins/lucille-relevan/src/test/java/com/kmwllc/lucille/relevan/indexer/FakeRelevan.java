package com.kmwllc.lucille.relevan.indexer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.List;
import java.util.stream.Collectors;

/**
 * An in-process stand-in for the Relevan API. It records every request and answers with queued responses, falling back
 * to a successful default for each endpoint.
 */
class FakeRelevan implements AutoCloseable {

  record Request(String method, String path, String apiKey, JsonNode body) {}

  private record Reply(String method, String pathSuffix, int status, String body) {}

  static final String ORG = "test-org";

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final HttpServer server;
  private final List<Request> requests = Collections.synchronizedList(new ArrayList<>());
  private final Deque<Reply> queued = new ArrayDeque<>();
  private volatile String mapping = "{\"fields\":{\"title\":{\"use\":[\"searchable\"]}}}";

  FakeRelevan() throws IOException {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/", this::handle);
    server.start();
  }

  String url() {
    return "http://127.0.0.1:" + server.getAddress().getPort();
  }

  /** Answers the next request whose method matches and whose path ends with {@code pathSuffix} with this response. */
  synchronized FakeRelevan reply(String method, String pathSuffix, int status, String body) {
    queued.addLast(new Reply(method, pathSuffix, status, body));
    return this;
  }

  /** Sets the mapping returned as the latest revision by the mappings endpoint; null means no mapping. */
  FakeRelevan mapping(String mappingJson) {
    this.mapping = mappingJson;
    return this;
  }

  List<Request> requests() {
    synchronized (requests) {
      return new ArrayList<>(requests);
    }
  }

  List<Request> requests(String method, String pathSuffix) {
    return requests().stream()
        .filter(r -> r.method().equals(method) && r.path().endsWith(pathSuffix))
        .collect(Collectors.toList());
  }

  private void handle(HttpExchange exchange) throws IOException {
    String method = exchange.getRequestMethod();
    String path = exchange.getRequestURI().getRawPath();
    byte[] raw = exchange.getRequestBody().readAllBytes();
    JsonNode body = raw.length == 0 ? null : MAPPER.readTree(raw);
    requests.add(new Request(method, path, exchange.getRequestHeaders().getFirst("x-api-key"), body));

    Reply reply = nextQueued(method, path);
    if (reply == null) {
      reply = defaultReply(method, path, body);
    }

    byte[] response = reply.body().getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("content-type", "application/json");
    exchange.sendResponseHeaders(reply.status(), response.length == 0 ? -1 : response.length);
    try (OutputStream out = exchange.getResponseBody()) {
      out.write(response);
    }
  }

  private synchronized Reply nextQueued(String method, String path) {
    for (Reply reply : queued) {
      if (reply.method().equals(method) && path.endsWith(reply.pathSuffix())) {
        queued.remove(reply);
        return reply;
      }
    }
    return null;
  }

  private Reply defaultReply(String method, String path, JsonNode body) {
    if (path.equals("/v1/organizations")) {
      return new Reply(method, path, 200, "[{\"id\":\"" + ORG + "\",\"slug\":\"" + ORG + "\",\"name\":\"Test\"}]");
    }
    if (path.endsWith("/documents/_bulk")) {
      int count = body.get("documents").size();
      return new Reply(method, path, 202, "{\"accepted\":" + count + ",\"documentIds\":[]}");
    }
    if (path.endsWith("/mappings")) {
      String revisions = mapping == null ? "[]" : "[{\"revision\":1,\"relevanMapping\":{\"fields\":{}}},"
          + "{\"revision\":2,\"relevanMapping\":" + mapping + "}]";
      return new Reply(method, path, 200, revisions);
    }
    if (method.equals("DELETE")) {
      return new Reply(method, path, 200, "{\"message\":\"queued for deletion\"}");
    }
    return new Reply(method, path, 200, "{\"indexName\":\"songs\",\"status\":\"ready\"}");
  }

  @Override
  public void close() {
    server.stop(0);
  }
}
