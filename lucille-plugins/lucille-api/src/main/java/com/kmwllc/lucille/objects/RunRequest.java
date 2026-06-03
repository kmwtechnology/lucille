package com.kmwllc.lucille.objects;

import jakarta.validation.constraints.NotBlank;

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
