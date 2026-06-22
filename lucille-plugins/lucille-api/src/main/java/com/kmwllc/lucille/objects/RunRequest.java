package com.kmwllc.lucille.objects;

/**
 * A request to start a run in the Lucille API.
 */
public class RunRequest {
  private String configId;

  public String getConfigId() {
    return configId;
  }

  public void setConfigId(String configId) {
    this.configId = configId;
  }

}
