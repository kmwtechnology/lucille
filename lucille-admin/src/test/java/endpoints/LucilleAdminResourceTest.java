package endpoints;

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

  @Mock
  RunnerManager manager = RunnerManager.getInstance();

  @InjectMocks
  private static final LucilleAdminResource admin = new LucilleAdminResource();

  @Test
  public void testGetRunStatus() {
    when(manager.isRunning()).thenReturn(true);

    Response status = admin.getRunStatus();

    System.out.println(status.getEntity().toString());
  }

  @Test
  public void testStartRunLocal() {
  }

  @Test
  public void testStartRunNonLocal() {

  }

  @Test
  public void testRunWithoutParameters() {

  }

}
