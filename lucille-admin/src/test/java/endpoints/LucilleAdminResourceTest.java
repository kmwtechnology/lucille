package endpoints;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import jakarta.ws.rs.core.Response;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LucilleAdminResourceTest {

  private final RunnerManager runnerManager = RunnerManager.getInstance();

  @Test
  public void testGetRunStatus() {
    LucilleAdminResource admin = new LucilleAdminResource(runnerManager);
    Response status = admin.getRunStatus();

    assertEquals("{'isRunning': 'true', 'runId': ''}", status.getEntity().toString());
  }

  @Test
  public void testRun() {
    LucilleAdminResource admin = new LucilleAdminResource(runnerManager);

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
