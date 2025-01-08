package endpoints;

import static org.junit.Assert.assertEquals;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import com.kmwllc.lucille.AuthHandler;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleResource;

import io.dropwizard.auth.PrincipalImpl;
import jakarta.ws.rs.core.Response;

@RunWith(MockitoJUnitRunner.class)
public class LucilleResourceTest {

  
  private final RunnerManager runnerManager = RunnerManager.getInstance();
  private final Optional<PrincipalImpl> user = Optional.of(new PrincipalImpl("test"));
  private final String runId = "runId";
  private final AuthHandler authHandler = new AuthHandler(true);

  @Test
  public void testGetRunStatus() throws Exception {
    System.out.println(String.format("testGetRunStatus %s", runnerManager.getRunDetailsList()));
    runnerManager.waitForRunCompletion(runId);

    LucilleResource admin = new LucilleResource(runnerManager, authHandler);
//    Response response = admin.getRunStatus(user, runId);
//
//    System.out.println("=== Response Details ===");
//    System.out.println("Status: " + response.getStatus());
//    System.out.println("Status Info: " + response.getStatusInfo());
//
//    Object entity = response.getEntity();
//    if (entity != null) {
//        System.out.println("Body: " + entity.toString());
//    }
//
//    assertEquals("{error=Not Found, message=No run with the given ID was found., status=404}", response.getEntity().toString());
  }

  @Test
  public void testCreateRun() throws Exception {
    System.out.println(String.format("============>>>>testCreateRun %s", runnerManager.getRunDetailsList()));

    LucilleResource admin = new LucilleResource(runnerManager, authHandler);
    Response status = null;
    try {
      Map<String,Object> config = new HashMap<>();
      config.put("pipeline", "data");
      
      status = admin.createRun(user, config);
      System.out.println(String.format("testCreateRun %s", runnerManager.getRunDetailsList()));
    } catch (Exception e) {

    } finally {
      assertEquals(200, status.getStatus());
    }
  }

  @Test
  public void testRun() throws Exception {
    System.out.println(String.format("============>>>>testRun %s", runnerManager.getRunDetailsList()));

    System.out.println(String.format("testRun %s", runnerManager.getRunDetailsList()));
    runnerManager.waitForRunCompletion(runId);

    LucilleResource admin = new LucilleResource(runnerManager, authHandler);

    Response status = Response.status(404).build();
    try {
      Map<String,Object> config = new HashMap<>();
      config.put("pipeline", "data");
//      admin.createConfig
//      
//      status = admin.startRun(user, config);
      System.out.println(String.format("testRun %s", runnerManager.getRunDetailsList()));
    } catch (Exception e) {

    } finally {
      assertEquals(200, status.getStatus());
    }
  }
}
