package objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import com.kmwllc.lucille.objects.RunStatus;
import org.junit.Test;

public class RunStatusTest {

  @Test
  public void testEquals() {
    RunStatus status1 = new RunStatus("1234", true);
    RunStatus status2 = new RunStatus("1234", true);

    assertEquals(status1, status2);
  }

  @Test
  public void testNotEquals() {
    RunStatus notRunning = new RunStatus("", false);
    RunStatus running = new RunStatus("1234", true);

    assertNotEquals(running, notRunning);
  }

  @Test
  public void testToString() {
    RunStatus status = new RunStatus("1234", true);
    String statusStr = "{'isRunning': 'true', 'runId': '1234'}";

    assertEquals(statusStr, status.toString());
  }

}
