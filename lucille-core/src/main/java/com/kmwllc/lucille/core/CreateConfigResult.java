package com.kmwllc.lucille.core;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The result of creating a Config in the RunnerManager.
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class CreateConfigResult {

  private final String configId;

  private final String removedConfigId;

  /**
   * The result of creating a Config in the RunnerManager.
   * @param configId The ID of the newly created Config. Not null.
   * @param removedConfigId The ID of a Config that was removed as a result of this request. Null when no Config is removed.
   */
  @JsonCreator
  public CreateConfigResult(
      @JsonProperty("configId") String configId,
      @JsonProperty("removedConfigId") String removedConfigId
  ) {
    if (configId == null) {
      throw new IllegalArgumentException("Newly created configId cannot be null");
    }

    this.configId = configId;
    this.removedConfigId = removedConfigId;
  }

  /**
   * The result of creating a Config in the RunnerManager - and no Config was removed as a result.
   * @param configId The ID of the newly created Config. Not null.
   */
  public CreateConfigResult(String configId) {
    this(configId, null);
  }

  /**
   * @return The configId, which is not null.
   */
  public String getConfigId() {
    return configId;
  }

  /**
   * @return The removedConfigId, which may be null.
   */
  public String getRemovedConfigId() {
    return removedConfigId;
  }
}
