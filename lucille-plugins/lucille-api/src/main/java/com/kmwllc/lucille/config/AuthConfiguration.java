package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Configuration for authentication in the Lucille Admin API.
 * <p>
 * Supports BASIC_AUTH and NO_AUTH modes. Set properties via YAML config.
 */
public final class AuthConfiguration {

  /**
   * Default constructor for AuthConfiguration.
   * Required for deserialization and Javadoc compliance.
   */
  public AuthConfiguration() {
    // No-op constructor
  }

  /**
   * Supported authentication types for the Lucille Admin API.
   */
  public enum AuthType {
    /**
     * Basic authentication using a username and password.
     */
    BASIC_AUTH,
    /**
     * No authentication required.
     */
    NO_AUTH
  }

  /**
   * The authentication type to use. Defaults to NO_AUTH if not specified.
   */
  private AuthType type;

  /**
   * Password for BASIC_AUTH authentication.
   */
  private String password;
  
  /**
   * Whether authentication is enabled. Defaults to true.
   */
  private boolean enabled = true; 

  /**
   * Returns the authentication type.
   * @return AuthType the authentication type
   */
  @JsonProperty
  public AuthType getType() {
    return type;
  }

  /**
   * Sets the authentication type from a string. "basicAuth" sets BASIC_AUTH, otherwise NO_AUTH.
   * @param type the authentication type as a string
   */
  @JsonProperty
  public void setType(String type) {
    if (type.equals("basicAuth")) {
      this.type = AuthType.BASIC_AUTH;
    } else {
      this.type = AuthType.NO_AUTH;
    }
  }
  
  /**
   * Returns whether authentication is enabled.
   * @return true if enabled, false otherwise
   */
  @JsonProperty
  public boolean isEnabled() {
      return enabled;
  }

  /**
   * Sets whether authentication is enabled.
   * @param enabled true to enable, false to disable
   */
  @JsonProperty
  public void setEnabled(boolean enabled) {
      this.enabled = enabled;
  }  

  /**
   * Returns the password for BASIC_AUTH.
   * @return the password
   */
  @JsonProperty
  public String getPassword() {
    return password;
  }

  /**
   * Sets the password for BASIC_AUTH.
   * @param password the password
   */
  @JsonProperty
  public void setPassword(String password) {
    this.password = password;
  }
}
