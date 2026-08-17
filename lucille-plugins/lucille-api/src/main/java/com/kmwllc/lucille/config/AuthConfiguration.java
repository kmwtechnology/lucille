package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.auth.Auth;

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
   * The authentication type to use. Defaults to NO_AUTH if empty and must be BASIC_AUTH when authentication is enabled.
   */
  private AuthType type = AuthType.NO_AUTH;

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
   * Sets the authentication type from a string. "basicAuth" sets BASIC_AUTH and null or empty sets NO_AUTH.
   * @param type the authentication type as a string
   * @throws IllegalArgumentException if AuthType is not recognized
   */
  @JsonProperty
  public void setType(String type) {
    if (type == null || type.isEmpty()) {
      this.type = AuthType.NO_AUTH;
    } else if (type.equals("basicAuth")) {
      this.type = AuthType.BASIC_AUTH;
    } else {
      throw new IllegalArgumentException("Unsupported auth type configured for the Lucille Admin API: " + type);
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
