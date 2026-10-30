package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Test;
import static org.junit.Assert.*;

public class Py4JRuntimeTest {

  private static final String SCRIPT = "src/test/resources/ExternalPythonTest/process_document_1.py";

  private final List<Py4JRuntime> runtimes = new ArrayList<>();

  @After
  public void tearDown() throws StageException {
    for (Py4JRuntime runtime : runtimes) {
      runtime.stop();
    }
    runtimes.clear();
  }

  @Test
  public void testGatewayRejectsUnauthenticatedClient() throws Exception {
    Py4JRuntime runtime = startRuntime(null, SCRIPT);
    assertFalse("Connection without an auth token should be rejected", connectWithToken(runtime, null));
  }

  @Test
  public void testGatewayRejectsWrongToken() throws Exception {
    Py4JRuntime runtime = startRuntime(null, SCRIPT);
    assertFalse("Connection with the wrong auth token should be rejected", connectWithToken(runtime, "not-the-token"));
  }

  @Test
  public void testConcurrentRuntimesGetDistinctPorts() throws Exception {
    Py4JRuntime first = startRuntime(null, SCRIPT);
    Py4JRuntime second = startRuntime(null, SCRIPT);

    assertNotNull(first.getGatewayPort());
    assertNotNull(second.getGatewayPort());
    assertNotEquals(first.getGatewayPort(), second.getGatewayPort());
    assertTrue(first.isReady());
    assertTrue(second.isReady());
  }

  @Test
  public void testRequestedPortIsHonored() throws Exception {
    int requested = findFreePortPair();
    Py4JRuntime runtime = startRuntime(requested, SCRIPT);

    assertEquals(Integer.valueOf(requested), runtime.getGatewayPort());
    assertTrue(runtime.isReady());
  }

  @Test
  public void testFailedStartReleasesPorts() throws Exception {
    int requested = findFreePortPair();
    Py4JRuntime runtime = new Py4JRuntime("python3", "src/test/resources/ExternalPythonTest/does_not_exist.py", null, requested);
    runtimes.add(runtime);

    assertThrows(StageException.class, runtime::start);
    assertFalse(runtime.isReady());
    assertNull(runtime.getGatewayPort());
    assertTrue("Gateway port should be released after a failed start", awaitBindable(requested));
    assertTrue("Callback port should be released after a failed start", awaitBindable(requested + 1));
  }

  private Py4JRuntime startRuntime(Integer requestedPort, String script) throws StageException {
    Py4JRuntime runtime = new Py4JRuntime("python3", script, null, requestedPort);
    runtimes.add(runtime);
    runtime.start();
    return runtime;
  }

  // Runs the venv's Python with py4j against the gateway. Returns true only if the call succeeds.
  private boolean connectWithToken(Py4JRuntime runtime, String token) throws IOException, InterruptedException {
    String tokenArg = token == null ? "None" : "'" + token + "'";
    String code = "from py4j.java_gateway import JavaGateway, GatewayParameters\n"
        + "g = JavaGateway(gateway_parameters=GatewayParameters(address='127.0.0.1', port=" + runtime.getGatewayPort()
        + ", auth_token=" + tokenArg + ", read_timeout=5, auto_close=True))\n"
        + "g.jvm.System.currentTimeMillis()\n";
    Process process = new ProcessBuilder(runtime.getVenvPythonPath(), "-c", code).redirectErrorStream(true).start();
    return process.waitFor() == 0;
  }

  private static boolean isBindable(int port) {
    try (ServerSocket ignored = new ServerSocket(port)) {
      return true;
    } catch (IOException e) {
      return false;
    }
  }

  // Other test classes run in parallel forks and may briefly claim the same ephemeral port, so give the port a
  // short window to become bindable rather than checking it once.
  private static boolean awaitBindable(int port) throws InterruptedException {
    long deadline = System.currentTimeMillis() + 5_000;
    while (!isBindable(port)) {
      if (System.currentTimeMillis() >= deadline) {
        return false;
      }
      Thread.sleep(100);
    }
    return true;
  }

  // Finds a port p where both p and p + 1 are free, so both the gateway and callback ports can bind.
  private static int findFreePortPair() throws IOException {
    while (true) {
      try (ServerSocket socket = new ServerSocket(0)) {
        int port = socket.getLocalPort();
        if (port < 65535 && isBindable(port + 1)) {
          return port;
        }
      }
    }
  }
}
