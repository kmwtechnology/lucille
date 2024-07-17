package endpoints;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import jakarta.ws.rs.core.Response;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LucilleAdminResourceTest {

  private final RunnerManager runnerManager = RunnerManager.getInstance();

  @Before
  public void setUp() {
    try {
      Thread.sleep(500);
    } catch (InterruptedException e) {
    }
  }

  @Test
  public void testGetRunStatus() {
    LucilleAdminResource admin = new LucilleAdminResource(runnerManager);
    Response status = admin.getRunStatus();

    assertEquals("{'isRunning': 'false', 'runId': ''}", status.getEntity().toString());
  }

  @Test
  public void testRun() {
    LucilleAdminResource admin = new LucilleAdminResource(runnerManager);

    Response status = Response.status(404).build();
    try {
      status = admin.startRun();
    } catch (Exception e) {

    } finally {
      assertEquals(200, status.getStatus());
    }
  }
}
