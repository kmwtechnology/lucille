import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.Entity;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.server.model.Resource;
import org.glassfish.jersey.server.model.ResourceMethod;
import org.junit.ClassRule;
import org.junit.Test;

// Asserts that every endpoint the API registers requires authentication, except for an explicit allow list,
// and that valid credentials are accepted everywhere. Endpoints are discovered from Jersey's own resource
// model rather than listed here, so an endpoint added without authentication fails this test without anybody
// having to remember to extend it.
public class APIAuthCoverageTest {

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(
      APIApplication.class, ResourceHelpers.resourceFilePath("test-conf-auth-coverage.yml"));

  // The endpoints that are intentionally reachable without credentials. Adding to this set widens the
  // anonymously reachable surface of the API, so it should not happen without a good reason.
  private static final Set<String> PUBLIC_ENDPOINTS = Set.of("/swagger", "/openapi.json", "/v1/readyz", "/v1/livez");

  // Substituted for any path parameter, for example /v1/config/{configId}.
  private static final String PATH_PARAMETER_VALUE = "auth-coverage-test";

  private static final Pattern PATH_PARAMETER = Pattern.compile("\\{([^{}]+)}");

  private static final String CREDENTIALS =
      "Basic " + Base64.getEncoder().encodeToString("admin:password".getBytes());

  private final Client client = RULE.client();
  private final String url = String.format("http://localhost:%d", RULE.getLocalPort());

  private record Endpoint(String httpMethod, String path) {}

  @Test
  public void testAuthRequiredUnlessPublic() {
    for (Endpoint endpoint : discoverEndpoints()) {
      Response response = request(endpoint, false);
      int status = response.getStatus();

      if (PUBLIC_ENDPOINTS.contains(endpoint.path())) {
        assertNotEquals(endpoint + " is in PUBLIC_ENDPOINTS but requires authentication.", 401, status);
      } else {
        assertEquals(endpoint + " is reachable without authentication.", 401, status);
      }
    }
  }

  @Test
  public void testEveryEndpointAcceptsValidCredentials() {
    for (Endpoint endpoint : discoverEndpoints()) {
      assertNotEquals(endpoint + " rejected valid credentials.", 401, request(endpoint, true).getStatus());
    }
  }

  // Every resource method Jersey has registered, as a concrete HTTP method and path.
  private List<Endpoint> discoverEndpoints() {
    ResourceConfig resourceConfig = RULE.getEnvironment().jersey().getResourceConfig();

    Set<Class<?>> resourceClasses = new HashSet<>(resourceConfig.getClasses());
    resourceConfig.getInstances().forEach(instance -> resourceClasses.add(instance.getClass()));

    List<Endpoint> endpoints = new ArrayList<>();
    for (Class<?> resourceClass : resourceClasses) {
      Resource resource = Resource.from(resourceClass);

      if (resource != null) {
        collect(resource, "", endpoints);
      }
    }

    return endpoints;
  }

  private void collect(Resource resource, String parentPath, List<Endpoint> endpoints) {
    String path = parentPath + (resource.getPath() == null ? "" : resource.getPath());

    for (ResourceMethod method : resource.getResourceMethods()) {
      endpoints.add(new Endpoint(method.getHttpMethod(), concretePath(path)));
    }

    for (Resource child : resource.getChildResources()) {
      collect(child, path, endpoints);
    }
  }

  // Replaces path parameters with something requestable, so that /v1/run/{runId} can actually be called.
  private String concretePath(String template) {
    Matcher matcher = PATH_PARAMETER.matcher(template);
    StringBuilder path = new StringBuilder();

    while (matcher.find()) {
      String parameter = matcher.group(1);
      int regex = parameter.indexOf(':');
      // a parameter may constrain itself to a set of values, as in {type:json|yaml}
      String value = regex < 0 ? PATH_PARAMETER_VALUE : firstAllowedValue(parameter.substring(regex + 1));

      matcher.appendReplacement(path, Matcher.quoteReplacement(value));
    }
    matcher.appendTail(path);

    return path.toString();
  }

  private String firstAllowedValue(String regex) {
    String first = regex.trim().split("\\|")[0];

    return first.matches("[A-Za-z0-9._-]+") ? first : PATH_PARAMETER_VALUE;
  }

  private Response request(Endpoint endpoint, boolean withCredentials) {
    Invocation.Builder request = client.target(url + endpoint.path()).request();

    if (withCredentials) {
      request = request.header(HttpHeaders.AUTHORIZATION, CREDENTIALS);
    }

    return switch (endpoint.httpMethod()) {
      case "POST" -> request.post(Entity.entity("{}", MediaType.APPLICATION_JSON));
      case "PUT" -> request.put(Entity.entity("{}", MediaType.APPLICATION_JSON));
      case "DELETE" -> request.delete();
      default -> request.method(endpoint.httpMethod());
    };
  }
}
