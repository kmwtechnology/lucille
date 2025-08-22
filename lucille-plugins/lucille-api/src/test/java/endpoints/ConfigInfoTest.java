package endpoints;

import static org.junit.Assert.*;

import java.util.Optional;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.endpoints.ConfigInfo;

import jakarta.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;

public class ConfigInfoTest {

  private final ObjectMapper mapper = new ObjectMapper();
  private ConfigInfo resource;

  @Before
  public void setUp() {
    resource = new ConfigInfo(new AuthHandler(false));
  }

  @Test
  public void testConnectors_basicShape() throws Exception {
    Response resp = resource.getConnectors(Optional.empty());
    assertEquals(200, resp.getStatus());

    ArrayNode arr = mapper.valueToTree(resp.getEntity());
    assertTrue(arr.size() > 0);

    for (JsonNode n : arr) {
      assertTrue(n.has("class"));
      assertTrue(n.has("fields"));
      assertTrue(n.get("fields").isArray());
      assertFalse(n.has("paramsFromDescription"));
    }
  }

  @Test
  public void testIndexers_basicShape() throws Exception {
    Response resp = resource.getIndexers(Optional.empty());
    assertEquals(200, resp.getStatus());

    ArrayNode arr = mapper.valueToTree(resp.getEntity());
    assertTrue(arr.size() > 0);

    for (JsonNode n : arr) {
      assertTrue(n.has("class"));
      assertTrue(n.has("fields"));
      assertTrue(n.get("fields").isArray());
      assertFalse(n.has("paramsFromDescription"));
    }
  }

  @Test
  public void testStages_basicShape() throws Exception {
    Response resp = resource.getStages(Optional.empty());
    assertEquals(200, resp.getStatus());

    ArrayNode arr = mapper.valueToTree(resp.getEntity());
    assertTrue(arr.size() > 0);

    for (JsonNode n : arr) {
      assertTrue(n.has("class"));
      assertTrue(n.has("fields"));
      assertTrue(n.get("fields").isArray());
      assertFalse(n.has("paramsFromDescription"));
    }
  }
}