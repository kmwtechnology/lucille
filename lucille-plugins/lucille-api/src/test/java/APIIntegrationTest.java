import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.APIApplication;
import com.kmwllc.lucille.LucilleAPIConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.ClassRule;
import org.junit.Test;

public class APIIntegrationTest {

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(APIApplication.class, ResourceHelpers.resourceFilePath("test-conf.yml"));

  private final Client client = RULE.client();
  private final String url = String.format("http://localhost:%d/", RULE.getLocalPort());

  @Test
  public void testReadiness() {
    Response status = client.target(url + "readyz").request().get();

    assertEquals(200, status.getStatus());;
  }

  @Test
  public void testLiveness() {
    Response status = client.target(url + "livez").request().get();

    assertEquals(200, status.getStatus());
  }

  @Test
  public void testGetStoppedStatus() {
    Response status = client.target(url + "lucille").request().get();

    status.bufferEntity();
    byte[] bytes = ((ByteArrayInputStream) status.getEntity()).readAllBytes();
    String entity = new String(bytes, StandardCharsets.UTF_8);

    assertEquals("{\"runId\":\"\",\"running\":false}", entity);
  }
}
