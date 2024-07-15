import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.AdminAPI;
import com.kmwllc.lucille.LucilleAPIConfiguration;
import com.kmwllc.lucille.core.RunnerManager;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.apache.kafka.clients.admin.Admin;
import org.junit.ClassRule;
import org.junit.Test;
import org.mockito.Mock;

public class AdminIntegrationTest {

  @ClassRule
  public static final DropwizardAppRule<LucilleAPIConfiguration> RULE = new DropwizardAppRule<>(AdminAPI.class); //, ResourceHelpers.resourceFilePath("admin.yml"));

  @Mock
  RunnerManager rm = RunnerManager.getInstance();

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
    Response status = client.target(url + "admin").request().get();

    status.bufferEntity();
    byte[] bytes = ((ByteArrayInputStream) status.getEntity()).readAllBytes();
    String entity = new String(bytes, StandardCharsets.UTF_8);

    assertEquals("{\"runId\":\"\",\"running\":false}", entity);
  }

  @Test
  public void testLucilleStart() throws Exception {
    // TODO: Right now this test just ensures that running a POST to the /admin endpoint will call RunnerManager.run()
    // TODO : We should somehow mock the RunnerManager so that we can confirm getRunStatus() works before, during and after a lucille run
    when(rm.run()).thenReturn(true);

    try (Response status = client.target(url + "admin").request().post(null)) {
      status.bufferEntity();
      byte[] bytes = ((ByteArrayInputStream) status.getEntity()).readAllBytes();
      String entity = new String(bytes, StandardCharsets.UTF_8);

      assertEquals("Lucille run has been triggered.", entity);
    } catch (Exception e) {
      throw new Exception(e);
    }

  }

}
