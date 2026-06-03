package com.kmwllc.lucille.objects;

import jakarta.validation.constraints.NotBlank;

public class RunRequest {

  @NotBlank
  private String configId;

  private boolean lockConfig = false;

  public String getConfigId() {
    return configId;
  }

  public void setConfigId(String configId) {
    this.configId = configId;
  }

  public boolean isLockConfig() {
    return lockConfig;
  }

  public void setLockConfig(boolean lockConfig) {
    this.lockConfig = lockConfig;
  }
}
