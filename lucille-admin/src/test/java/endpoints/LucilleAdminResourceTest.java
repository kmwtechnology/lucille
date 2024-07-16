package endpoints;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import jakarta.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LucilleAdminResourceTest {

  private static final LucilleAdminResource admin = new LucilleAdminResource();

  @Test
  public void testGetRunStatus() {
    Response status = admin.getRunStatus();

    assertEquals("{'isRunning': 'false', 'runId': ''}", status.getEntity().toString());
  }

  @Test
  public void testRun() {
    // Ensure that a 200 OK is returned when the run is started
    Response status = Response.status(404).build();

    try {
      status = admin.startRun();
    } catch (Exception e) {
      System.out.println(e);
    }

    assertEquals(200, status.getStatus());
  }
}
