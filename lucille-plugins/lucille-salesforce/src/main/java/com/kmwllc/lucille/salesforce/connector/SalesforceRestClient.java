package com.kmwllc.lucille.salesforce.connector;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.salesforce.connector.SalesforceBlobClient.BinaryBody;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.FieldSummary;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.QueryPage;
import com.kmwllc.lucille.salesforce.connector.SalesforceClient.SObjectSummary;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Salesforce REST client. A configured access token is used as-is. Otherwise the client requests a token with the
 * username-password flow when a username is set, and the client-credentials flow when it is not.
 */
final class SalesforceRestClient implements SalesforceClient, SalesforceBlobClient {

  private static final Logger log = LoggerFactory.getLogger(SalesforceRestClient.class);
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private static final String USER_AGENT = "Lucille-SalesforceConnector";
  private static final int MAX_REDIRECTS = 5;

  private final HttpClient http;
  private final HttpClient binaryHttp;
  private final String loginUrl;
  private final String apiVersion;
  private final String clientId;
  private final String clientSecret;
  private final String username;
  private final String password;
  private final String configuredToken;
  private final String configuredInstanceUrl;
  private final int batchSize;
  private final boolean queryAll;
  private final int maxRetries;
  private final int requestTimeoutMs;

  private String accessToken;
  private String instanceUrl;
  private boolean connected;

  SalesforceRestClient(String loginUrl, String apiVersion, String clientId, String clientSecret, String username,
      String password, String accessToken, String instanceUrl, int batchSize, boolean queryAll, int maxRetries,
      int requestTimeoutMs) {
    this.http = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NORMAL)
        .build();
    this.binaryHttp = HttpClient.newBuilder()
        .connectTimeout(Duration.ofSeconds(30))
        .followRedirects(HttpClient.Redirect.NEVER)
        .build();
    this.loginUrl = loginUrl;
    this.apiVersion = apiVersion;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.username = username;
    this.password = password;
    this.configuredToken = accessToken;
    this.configuredInstanceUrl = instanceUrl;
    this.batchSize = batchSize;
    this.queryAll = queryAll;
    this.maxRetries = maxRetries;
    this.requestTimeoutMs = requestTimeoutMs;
  }

  @Override
  public void connect() throws IOException {
    if (connected && accessToken != null && instanceUrl != null) {
      return;
    }
    authenticate();
    connected = true;
  }

  @Override
  public String instanceUrl() {
    return instanceUrl;
  }

  @Override
  public List<SObjectSummary> listSObjects() throws IOException {
    JsonNode root = getJson(dataUrl("/sobjects"), false);
    JsonNode sobjects = root.get("sobjects");
    if (sobjects == null || !sobjects.isArray()) {
      throw new IOException("Salesforce sobjects response has no sobjects array.");
    }
    List<SObjectSummary> objects = new ArrayList<>();
    for (JsonNode node : sobjects) {
      if (!node.path("name").isTextual()) {
        continue;
      }
      String name = node.get("name").asText();
      objects.add(new SObjectSummary(
          name,
          node.path("label").asText(name),
          node.path("custom").asBoolean(false),
          node.path("queryable").asBoolean(false),
          node.path("deprecatedAndHidden").asBoolean(false),
          node.path("isInterface").asBoolean(false)));
    }
    return objects;
  }

  @Override
  public List<FieldSummary> describeFields(String sobject) throws IOException {
    JsonNode root = getJson(dataUrl("/sobjects/" + urlEncode(sobject) + "/describe"), false);
    JsonNode fields = root.get("fields");
    if (fields == null || !fields.isArray()) {
      throw new IOException("Salesforce describe for " + sobject + " has no fields array.");
    }
    List<FieldSummary> summaries = new ArrayList<>();
    for (JsonNode node : fields) {
      if (!node.path("name").isTextual()) {
        continue;
      }
      summaries.add(new FieldSummary(
          node.get("name").asText(),
          node.path("type").isTextual() ? node.get("type").asText() : null,
          node.path("deprecatedAndHidden").asBoolean(false)));
    }
    return summaries;
  }

  @Override
  public QueryPage query(String soql) throws IOException {
    String resource = queryAll ? "/queryAll?q=" : "/query?q=";
    return toPage(getJson(dataUrl(resource + urlEncode(soql)), true));
  }

  @Override
  public QueryPage queryMore(String nextRecordsUrl) throws IOException {
    return toPage(getJson(resolve(nextRecordsUrl), false));
  }

  @Override
  public BinaryBody downloadBlob(String sobject, String id, String blobField, long maxBytes) throws IOException {
    if (sobject == null || sobject.isBlank() || id == null || id.isBlank() || blobField == null || blobField.isBlank()) {
      throw new IllegalArgumentException("sobject, id, and blobField are required to download a Salesforce file.");
    }
    if (maxBytes < 1) {
      throw new IllegalArgumentException("maxBytes must be positive, got " + maxBytes + ".");
    }
    ensureConnected();
    String url = dataUrl("/sobjects/" + urlEncode(sobject) + "/" + urlEncode(id) + "/" + urlEncode(blobField));
    return download(url, maxBytes);
  }

  private BinaryBody download(String url, long maxBytes) throws IOException {
    String current = url;
    int redirects = 0;
    int retriesLeft = maxRetries;
    boolean refreshed = false;
    while (true) {
      HttpResponse<InputStream> response;
      try {
        response = binaryHttp.send(binaryGet(current), HttpResponse.BodyHandlers.ofInputStream());
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Salesforce request interrupted.", e);
      } catch (IOException e) {
        if (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {
          retriesLeft--;
          log.warn("Salesforce request to {} failed ({}); retrying.", pathOf(current), e.getMessage());
          pause(backoffMillis(maxRetries - retriesLeft));
          continue;
        }
        throw new IOException("Salesforce request to " + pathOf(current) + " failed: " + e.getMessage(), e);
      }
      int status = response.statusCode();
      if (isRedirect(status)) {
        String location = response.headers().firstValue("Location").orElse(null);
        closeQuietly(response.body());
        if (location == null || location.isBlank()) {
          throw new IOException("Salesforce redirect from " + pathOf(current) + " had no Location header.");
        }
        if (redirects >= MAX_REDIRECTS) {
          throw new IOException("Salesforce file download exceeded " + MAX_REDIRECTS + " redirects.");
        }
        redirects++;
        current = resolveRedirect(current, location.trim());
        continue;
      }
      if (status >= 200 && status < 300) {
        long advertised = response.headers().firstValueAsLong("Content-Length").orElse(-1);
        if (advertised > maxBytes) {
          closeQuietly(response.body());
          throw new IOException("Salesforce file is " + advertised + " bytes, over the maxBytes limit of " + maxBytes + ".");
        }
        byte[] bytes = readLimited(response.body(), maxBytes);
        return new BinaryBody(bytes, contentType(response));
      }
      String errorBody = readError(response.body());
      if (status == 401 && !refreshed && configuredToken == null) {
        log.info("Salesforce rejected the session; requesting a new access token.");
        authenticate();
        refreshed = true;
        current = url;
        redirects = 0;
        continue;
      }
      if (isRetryable(status) && retriesLeft > 0) {
        retriesLeft--;
        log.warn("Salesforce request to {} returned HTTP {}; retrying.", pathOf(current), status);
        pause(retryDelayMillis(response, maxRetries - retriesLeft));
        continue;
      }
      throw new IOException("Salesforce request to " + pathOf(current) + " failed with HTTP " + status + ": "
          + errorMessage(errorBody));
    }
  }

  private HttpRequest binaryGet(String url) {
    return HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofMillis(requestTimeoutMs))
        .header("Accept", "*/*")
        .header("Authorization", "Bearer " + accessToken)
        .header("User-Agent", USER_AGENT)
        .GET()
        .build();
  }

  @Override
  public void close() {
    http.close();
    binaryHttp.close();
  }

  private void authenticate() throws IOException {
    if (configuredToken != null) {
      accessToken = configuredToken;
      instanceUrl = stripTrailingSlash(configuredInstanceUrl);
      return;
    }
    String form = username != null ? passwordGrant() : clientCredentialsGrant();
    HttpRequest request = HttpRequest.newBuilder(URI.create(loginUrl + "/services/oauth2/token"))
        .timeout(Duration.ofMillis(requestTimeoutMs))
        .header("Content-Type", "application/x-www-form-urlencoded")
        .header("Accept", "application/json")
        .header("User-Agent", USER_AGENT)
        .POST(HttpRequest.BodyPublishers.ofString(form))
        .build();
    JsonNode json = send(() -> request, false);
    if (!json.path("access_token").isTextual() || !json.path("instance_url").isTextual()) {
      throw new IOException("Salesforce token response did not include access_token and instance_url.");
    }
    accessToken = json.get("access_token").asText();
    instanceUrl = stripTrailingSlash(json.get("instance_url").asText());
    log.info("Authenticated to Salesforce at {}.", instanceUrl);
  }

  private String passwordGrant() {
    return "grant_type=password"
        + "&client_id=" + urlEncode(clientId)
        + "&client_secret=" + urlEncode(clientSecret)
        + "&username=" + urlEncode(username)
        + "&password=" + urlEncode(password);
  }

  private String clientCredentialsGrant() {
    return "grant_type=client_credentials"
        + "&client_id=" + urlEncode(clientId)
        + "&client_secret=" + urlEncode(clientSecret);
  }

  private JsonNode getJson(String url, boolean queryBatch) throws IOException {
    ensureConnected();
    return send(() -> authorizedGet(url, queryBatch), true);
  }

  private HttpRequest authorizedGet(String url, boolean queryBatch) {
    HttpRequest.Builder builder = HttpRequest.newBuilder(URI.create(url))
        .timeout(Duration.ofMillis(requestTimeoutMs))
        .header("Accept", "application/json")
        .header("Authorization", "Bearer " + accessToken)
        .header("User-Agent", USER_AGENT)
        .GET();
    if (queryBatch) {
      builder.header("Sforce-Query-Options", "batchSize=" + batchSize);
    }
    return builder.build();
  }

  private void ensureConnected() throws IOException {
    if (accessToken == null || instanceUrl == null) {
      throw new IOException("Salesforce client is not connected.");
    }
  }

  private JsonNode send(Supplier<HttpRequest> requests, boolean allowReauth) throws IOException {
    int retriesLeft = maxRetries;
    boolean refreshed = false;
    while (true) {
      HttpRequest request = requests.get();
      HttpResponse<String> response;
      try {
        response = sendOnce(request);
      } catch (IOException e) {
        if (retriesLeft > 0 && !Thread.currentThread().isInterrupted()) {
          retriesLeft--;
          log.warn("Salesforce request to {} failed ({}); retrying.", pathOf(request), e.getMessage());
          pause(backoffMillis(maxRetries - retriesLeft));
          continue;
        }
        throw new IOException("Salesforce request to " + pathOf(request) + " failed: " + e.getMessage(), e);
      }
      int status = response.statusCode();
      if (status >= 200 && status < 300) {
        return readBody(response.body());
      }
      if (status == 401 && allowReauth && !refreshed && configuredToken == null) {
        log.info("Salesforce rejected the session; requesting a new access token.");
        authenticate();
        refreshed = true;
        continue;
      }
      if (isRetryable(status) && retriesLeft > 0) {
        retriesLeft--;
        log.warn("Salesforce request to {} returned HTTP {}; retrying.", pathOf(request), status);
        pause(retryDelayMillis(response, maxRetries - retriesLeft));
        continue;
      }
      throw new IOException("Salesforce request to " + pathOf(request) + " failed with HTTP " + status + ": "
          + errorMessage(response.body()));
    }
  }

  private HttpResponse<String> sendOnce(HttpRequest request) throws IOException {
    try {
      return http.send(request, HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Salesforce request interrupted.", e);
    }
  }

  private static boolean isRetryable(int status) {
    return status == 408 || status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
  }

  private static long retryDelayMillis(HttpResponse<?> response, int attempt) {
    return response.headers().firstValue("Retry-After")
        .map(SalesforceRestClient::retryAfterMillis)
        .orElseGet(() -> backoffMillis(attempt));
  }

  private static long retryAfterMillis(String header) {
    try {
      long seconds = Long.parseLong(header.trim());
      return Math.min(Math.max(seconds, 0), 30) * 1000L;
    } catch (NumberFormatException e) {
      return backoffMillis(1);
    }
  }

  private static long backoffMillis(int attempt) {
    int shift = Math.min(Math.max(attempt, 1) - 1, 4);
    return Math.min(1000L << shift, 30_000L);
  }

  private static void pause(long millis) throws IOException {
    if (millis <= 0) {
      return;
    }
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while waiting to retry a Salesforce request.", e);
    }
  }

  private static JsonNode readBody(String body) throws IOException {
    if (body == null || body.isBlank()) {
      throw new IOException("Salesforce returned an empty body.");
    }
    try {
      return MAPPER.readTree(body);
    } catch (JsonProcessingException e) {
      throw new IOException("Salesforce returned a body that is not JSON.", e);
    }
  }

  private static QueryPage toPage(JsonNode root) throws IOException {
    if (root == null || !root.isObject()) {
      throw new IOException("Salesforce query response was not an object: " + errorMessage(root == null ? "" : root.toString()));
    }
    int total = root.path("totalSize").asInt(-1);
    boolean done = !root.has("done") || root.get("done").asBoolean(true);
    String next = root.path("nextRecordsUrl").isTextual() ? root.get("nextRecordsUrl").asText() : null;
    List<JsonNode> records = new ArrayList<>();
    JsonNode rows = root.get("records");
    if (rows != null && rows.isArray()) {
      rows.forEach(records::add);
    }
    return new QueryPage(total, done, next, records);
  }

  static String errorMessage(String body) {
    if (body == null || body.isBlank()) {
      return "empty response body";
    }
    try {
      JsonNode node = MAPPER.readTree(body);
      if (node.isArray() && !node.isEmpty()) {
        return joinError(node.get(0));
      }
      if (node.has("error_description")) {
        String error = node.path("error").asText("");
        String description = node.path("error_description").asText();
        return error.isBlank() ? description : error + ": " + description;
      }
      if (node.has("message")) {
        return joinError(node);
      }
    } catch (JsonProcessingException e) {
      // The body is not JSON; return a short slice of it below.
    }
    return body.length() > 500 ? body.substring(0, 500) : body;
  }

  private static String joinError(JsonNode node) {
    String code = node.path("errorCode").asText("");
    String message = node.path("message").asText("");
    if (code.isBlank()) {
      return message.isBlank() ? node.toString() : message;
    }
    return message.isBlank() ? code : code + ": " + message;
  }

  private String dataUrl(String suffix) {
    return instanceUrl + "/services/data/" + apiVersion + suffix;
  }

  private String resolve(String pathOrUrl) {
    if (pathOrUrl.startsWith("https://") || pathOrUrl.startsWith("http://")) {
      return pathOrUrl;
    }
    if (!pathOrUrl.startsWith("/")) {
      pathOrUrl = "/" + pathOrUrl;
    }
    return instanceUrl + pathOrUrl;
  }

  private static String urlEncode(String value) {
    return URLEncoder.encode(value, StandardCharsets.UTF_8);
  }

  private static String stripTrailingSlash(String url) {
    String stripped = url;
    while (stripped.endsWith("/")) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    return stripped;
  }

  private static String pathOf(HttpRequest request) {
    return pathOf(request.uri().toString());
  }

  private static String pathOf(String url) {
    try {
      String path = URI.create(url).getPath();
      return path == null || path.isBlank() ? url : path;
    } catch (IllegalArgumentException e) {
      return url;
    }
  }

  private static boolean isRedirect(int status) {
    return status == 301 || status == 302 || status == 303 || status == 307 || status == 308;
  }

  private static String resolveRedirect(String current, String location) throws IOException {
    URI resolved;
    try {
      resolved = URI.create(current).resolve(location);
    } catch (IllegalArgumentException e) {
      throw new IOException("Salesforce file download redirected to an invalid URL.", e);
    }
    String scheme = resolved.getScheme();
    if (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) {
      throw new IOException("Salesforce file download redirected to a non-http URL.");
    }
    return resolved.toString();
  }

  private static byte[] readLimited(InputStream in, long maxBytes) throws IOException {
    try (in) {
      ByteArrayOutputStream out = new ByteArrayOutputStream();
      byte[] buf = new byte[8192];
      long total = 0;
      int read;
      while ((read = in.read(buf)) >= 0) {
        total += read;
        if (total > maxBytes) {
          throw new IOException("Salesforce file is larger than maxBytes (" + maxBytes + ").");
        }
        out.write(buf, 0, read);
      }
      return out.toByteArray();
    }
  }

  private static String readError(InputStream in) {
    if (in == null) {
      return "";
    }
    try (in) {
      return new String(in.readNBytes(4096), StandardCharsets.UTF_8);
    } catch (IOException e) {
      return "";
    }
  }

  private static void closeQuietly(InputStream in) {
    if (in == null) {
      return;
    }
    try {
      in.close();
    } catch (IOException e) {
      // The response body is being discarded.
    }
  }

  private static String contentType(HttpResponse<?> response) {
    String raw = response.headers().firstValue("Content-Type").orElse(null);
    if (raw == null || raw.isBlank()) {
      return null;
    }
    int semi = raw.indexOf(';');
    String type = (semi >= 0 ? raw.substring(0, semi) : raw).trim();
    return type.isEmpty() ? null : type;
  }
}
