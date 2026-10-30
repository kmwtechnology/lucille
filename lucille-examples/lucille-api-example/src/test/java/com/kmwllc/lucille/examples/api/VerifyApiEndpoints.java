package com.kmwllc.lucille.examples.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.net.http.HttpResponse;

import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * Checks the lucille-api container's endpoints over HTTP: health probes, auth, config-info, and rejection of an
 * invalid config.
 * <p>
 * Not picked up by the normal build (the name doesn't match surefire's default includes). It's run by the
 * verifier container in docker-compose.yml.
 */
public class VerifyApiEndpoints {

  @Test
  public void testProbes() throws Exception {
    assertEquals(200, ApiClient.get("/v1/livez", false).statusCode());
    assertEquals(200, ApiClient.get("/v1/readyz", false).statusCode());
  }

  // auth is enabled in conf/api.yml, so protected endpoints must reject requests without credentials
  @Test
  public void testAuthEnforced() throws Exception {
    assertEquals(401, ApiClient.get("/v1/run", false).statusCode());
    assertEquals(200, ApiClient.get("/v1/run", true).statusCode());
  }

  // component scanning must work inside the shaded jar
  @Test
  public void testConnectorList() throws Exception {
    HttpResponse<String> response = ApiClient.get("/v1/config-info/connector-list", true);
    assertEquals(200, response.statusCode());
    JsonNode connectors = ApiClient.json(response);
    assertTrue("connector-list returned no connectors", connectors.isArray() && !connectors.isEmpty());
  }

  @Test
  public void testInvalidConfigRejected() throws Exception {
    assertEquals(400, ApiClient.postConfig("{ not valid", "application/hocon").statusCode());
  }
}
