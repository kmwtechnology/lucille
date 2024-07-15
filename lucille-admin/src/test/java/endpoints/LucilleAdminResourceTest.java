package endpoints;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import jakarta.ws.rs.core.Response;
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

  // TODO : Do we need to test the startRun() method here if we are testing it in the integration test
}
