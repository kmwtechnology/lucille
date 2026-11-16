package com.kmwllc.lucille.salesforce.connector;

import com.kmwllc.lucille.core.ConfigUtils;
import com.typesafe.config.Config;
import java.net.URI;

/**
 * Shared login settings for the connector and the file stage. A configured access token is used as-is. Otherwise the
 * client requests a token with the username-password flow when a username is set, and the client-credentials flow when
 * it is not.
 */
final class SalesforceAuth {

  private static final String DEFAULT_LOGIN_URL = "https://login.salesforce.com";
  private static final int DEFAULT_MAX_RETRIES = 3;
  private static final int DEFAULT_TIMEOUT_MS = 120_000;

  private SalesforceAuth() {
  }

  record Settings(String loginUrl, String apiVersion, String clientId, String clientSecret, String username,
      String password, String accessToken, String instanceUrl, int requestTimeoutMs, int maxRetries) {

    SalesforceRestClient open(int batchSize, boolean queryAll) {
      return new SalesforceRestClient(loginUrl, apiVersion, clientId, clientSecret, username, password, accessToken,
          instanceUrl, batchSize, queryAll, maxRetries, requestTimeoutMs);
    }
  }

  static Settings from(Config config) {
    String accessToken = blankToNull(ConfigUtils.getStringOrDefault(config, "accessToken", null));
    String instanceUrl = blankToNull(ConfigUtils.getStringOrDefault(config, "instanceUrl", null));
    String clientId = blankToNull(ConfigUtils.getStringOrDefault(config, "clientId", null));
    String clientSecret = blankToNull(ConfigUtils.getStringOrDefault(config, "clientSecret", null));
    String username = blankToNull(ConfigUtils.getStringOrDefault(config, "username", null));
    String password = blankToNull(ConfigUtils.getStringOrDefault(config, "password", null));
    String loginUrl = requireUrl("loginUrl", ConfigUtils.getStringOrDefault(config, "loginUrl", DEFAULT_LOGIN_URL));
    if (instanceUrl != null) {
      instanceUrl = requireUrl("instanceUrl", instanceUrl);
    }
    String apiVersion = normalizeApiVersion(
        ConfigUtils.getStringOrDefault(config, "apiVersion", SalesforceConnector.DEFAULT_API_VERSION));
    validate(accessToken, instanceUrl, clientId, clientSecret, username, password);

    int requestTimeoutMs = ConfigUtils.getIntOrDefault(config, "requestTimeoutMs", DEFAULT_TIMEOUT_MS);
    if (requestTimeoutMs <= 0) {
      throw new IllegalArgumentException("requestTimeoutMs must be positive, got " + requestTimeoutMs + ".");
    }
    int maxRetries = ConfigUtils.getIntOrDefault(config, "maxRetries", DEFAULT_MAX_RETRIES);
    if (maxRetries < 0) {
      throw new IllegalArgumentException("maxRetries must not be negative, got " + maxRetries + ".");
    }
    return new Settings(loginUrl, apiVersion, clientId, clientSecret, username, password, accessToken, instanceUrl,
        requestTimeoutMs, maxRetries);
  }

  private static void validate(String accessToken, String instanceUrl, String clientId, String clientSecret,
      String username, String password) {
    if (accessToken != null) {
      if (instanceUrl == null) {
        throw new IllegalArgumentException("instanceUrl is required when accessToken is set.");
      }
      return;
    }
    if (clientId == null || clientSecret == null) {
      throw new IllegalArgumentException(
          "Set accessToken and instanceUrl, or set clientId and clientSecret. Add username and password for the username-password flow.");
    }
    if (username == null && password != null) {
      throw new IllegalArgumentException("password is set without username.");
    }
    if (username != null && password == null) {
      throw new IllegalArgumentException(
          "username is set without password. Append the Salesforce security token when the org requires one.");
    }
  }

  static String normalizeApiVersion(String raw) {
    if (raw == null || raw.isBlank()) {
      throw new IllegalArgumentException("apiVersion is blank.");
    }
    String version = raw.trim();
    if (version.startsWith("v") || version.startsWith("V")) {
      version = version.substring(1);
    }
    if (!version.matches("\\d+\\.\\d+")) {
      throw new IllegalArgumentException("apiVersion must look like 67.0, got \"" + raw + "\".");
    }
    return "v" + version;
  }

  private static String requireUrl(String name, String url) {
    if (url == null || url.isBlank()) {
      throw new IllegalArgumentException(name + " is blank.");
    }
    String stripped = url.trim();
    while (stripped.endsWith("/")) {
      stripped = stripped.substring(0, stripped.length() - 1);
    }
    if (!stripped.startsWith("https://") && !stripped.startsWith("http://")) {
      throw new IllegalArgumentException(name + " must be an http or https URL.");
    }
    try {
      URI.create(stripped);
    } catch (IllegalArgumentException e) {
      throw new IllegalArgumentException(name + " is not a valid URL.", e);
    }
    return stripped;
  }

  private static String blankToNull(String value) {
    return value == null || value.isBlank() ? null : value.trim();
  }
}
