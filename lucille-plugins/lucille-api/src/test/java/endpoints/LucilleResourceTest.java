package endpoints;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;
import jakarta.ws.rs.core.Response;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class LucilleResourceTest {

  private final RunnerManager runnerManager = RunnerManager.getInstance();

  @Test
  public void testGetRunStatus() throws InterruptedException, ExecutionException {
    runnerManager.waitForRunCompletion();

    LucilleResource admin = new LucilleResource(runnerManager);
    Response status = admin.getRunStatus();

    assertEquals("{'isRunning': 'false', 'runId': ''}", status.getEntity().toString());
  }

  @Test
  public void testRun() throws InterruptedException, ExecutionException {
    runnerManager.waitForRunCompletion();

    LucilleResource admin = new LucilleResource(runnerManager);
    Response status = admin.startRun();
    assertEquals(200, status.getStatus());
    status.close();
  }
}
