package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configure Authentication for the Lucille Admin API
 */
public final class AuthConfiguration {

  public enum AuthType {
    BASIC_AUTH,
    NO_AUTH
  }

  private AuthType type;

  private String password;
  
  private boolean enabled = true; 

  @JsonProperty
  public AuthType getType() {
    return type;
  }

  @JsonProperty
  public void setType(String type) {
    if (type.equals("basicAuth")) {
      this.type = AuthType.BASIC_AUTH;
    } else {
      this.type = AuthType.NO_AUTH;
    }
  }
  
  @JsonProperty
  public boolean isEnabled() {
      return enabled;
  }

  @JsonProperty
  public void setEnabled(boolean enabled) {
      this.enabled = enabled;
  }  

  @JsonProperty
  public String getPassword() {
    return password;
  }

  @JsonProperty
  public void setPassword(String password) {
    this.password = password;
  }
}
