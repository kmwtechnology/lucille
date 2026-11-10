package com.kmwllc.lucille.api.objects;

/**
 * Describes the API's authentication state to unauthenticated clients, so the Admin UI knows
 * whether to show a login screen.
 */
public class AuthInfoResponse {
  private boolean authEnabled;

  /**
   * Default constructor for deserialization.
   */
  public AuthInfoResponse() {
  }

  /**
   * Constructs an AuthInfoResponse.
   * @param authEnabled whether the API requires authentication
   */
  public AuthInfoResponse(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }

  public boolean isAuthEnabled() {
    return authEnabled;
  }

  public void setAuthEnabled(boolean authEnabled) {
    this.authEnabled = authEnabled;
  }
}
