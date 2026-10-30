package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import java.util.Objects;

public final class Py4JRuntimeManager {

  private static final Py4JRuntimeManager instance = new Py4JRuntimeManager();

  private static Py4JRuntime runtime;
  private int refCount = 0;

  private Py4JRuntimeManager() {
  }

  public static Py4JRuntimeManager getInstance() {
    return instance;
  }

  public synchronized Py4JRuntime acquire(String pythonExecutable, String scriptPath, String requirementsPath, Integer port)
      throws StageException {
    if (runtime == null) {
      // only publish the runtime to the shared field once it has started, so a failed start can never leave a broken runtime behind
      Py4JRuntime newRuntime = new Py4JRuntime(pythonExecutable, scriptPath, requirementsPath, port);
      try {
        newRuntime.start();
      } catch (Exception e) {
        // Cleanup the runtime before rethrowing the error, so future invocations can retry Runtime creation
        try {
          newRuntime.stop();
        } catch (StageException stopError) {
          e.addSuppressed(stopError);
        }
        throw e;
      }
      runtime = newRuntime;
    } else {
      if (!(Objects.equals(pythonExecutable, runtime.getPythonExecutable()) && Objects.equals(scriptPath, runtime.getScriptPath()) &&
      Objects.equals(requirementsPath, runtime.getRequirementsPath()) && Objects.equals(port, runtime.getRequestedPort()))) {
        throw new StageException("Illegal attempt to acquire Py4JRuntime with different parameters from the initialized instance.\n" +
            "Provided Params: " + pythonExecutable + " " + scriptPath + " " + requirementsPath + " " + port + " \n" +
            "Original Params: " + runtime.getPythonExecutable() + " " + runtime.getScriptPath() + " " + runtime.getRequirementsPath() + " " + runtime.getRequestedPort());
      }
    }

    refCount++;
    return runtime;
  }

  public synchronized void release() throws StageException {
    if (runtime == null) {
      return;
    }

    refCount = Math.max(0, refCount - 1);
    if (refCount == 0) {
      try {
        runtime.stop();
      } finally {
        runtime = null;
      }
    }
  }
}
