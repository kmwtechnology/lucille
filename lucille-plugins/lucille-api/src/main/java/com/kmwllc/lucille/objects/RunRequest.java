package com.kmwllc.lucille.objects;

import jakarta.validation.constraints.NotBlank;

/**
 * A request to start a run in the Lucille API.
 */
public class RunRequest {

  @NotBlank
  private String configId;

  public String getConfigId() {
    return configId;
  }

  public void setConfigId(String configId) {
    this.configId = configId;
  }

}
