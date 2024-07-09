package com.kmwllc.lucille.core;

public class RunnerManager {

  private static RunnerManager instance = null;

  private boolean isRunning;

  private RunnerManager() {}

  public static RunnerManager getInstance() {

    if (instance == null) {
      instance = new RunnerManager();
    }

    return instance;
  }

  public boolean isRunning() {
    return isRunning;
  }

  protected void setIsRunning(boolean isRunning) {
    this.isRunning = isRunning;
  }

  public boolean start() {
    return false;
  }
}
