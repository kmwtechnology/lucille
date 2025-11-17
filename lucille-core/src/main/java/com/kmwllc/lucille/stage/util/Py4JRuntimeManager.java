package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;

public final class Py4JRuntimeManager {

  private static final Py4JRuntimeManager instance = new Py4JRuntimeManager();

  private static Py4JRuntime runtime;
  private int refCount;

  private String pythonExecutable;
  private String scriptPath;
  private String requirementsPath;
  private Integer port;

  private Py4JRuntimeManager() {
  }

  public static Py4JRuntimeManager getInstance() {
    return instance;
  }

  public synchronized Py4JRuntime aquire(
      String pythonExecutable,
      String scriptPath,
      String requirementsPath,
      Integer port
  ) throws StageException {
    if (runtime == null) {
      runtime = new Py4JRuntime(pythonExecutable, scriptPath, requirementsPath, port);
      runtime.start();

      System.out.println("RUNTIME STARTED");

      this.pythonExecutable = pythonExecutable;
      this.scriptPath = scriptPath;
      this.requirementsPath = requirementsPath;
      this.port = port;
    }

    refCount++;
    System.out.println("REFS COUNT " + refCount);
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
        pythonExecutable = null;
        scriptPath = null;
        requirementsPath = null;
        port = null;
      }
    }
  }
}
