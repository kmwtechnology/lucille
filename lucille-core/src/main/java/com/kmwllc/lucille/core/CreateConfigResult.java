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

  /**
   * The result of creating a Config in the RunnerManager. (Currently, just the config's ID.)
   * @param configId The ID of the newly created Config. Not null.
   */
  @JsonCreator
  public CreateConfigResult(
      @JsonProperty("configId") String configId
  ) {
    if (configId == null) {
      throw new IllegalArgumentException("Newly created configId cannot be null");
    }

    this.configId = configId;
  }

  /**
   * @return The configId, which is not null.
   */
  public String getConfigId() {
    return configId;
  }
}
