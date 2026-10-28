package com.kmwllc.lucille.objects;

/**
 * A successful login response carrying the issued bearer token.
 */
public class LoginResponse {
  private String token;

  /**
   * Default constructor for deserialization.
   */
  public LoginResponse() {
  }

  /**
   * Constructs a LoginResponse with the given token.
   * @param token the issued bearer token
   */
  public LoginResponse(String token) {
    this.token = token;
  }

  public String getToken() {
    return token;
  }

  public void setToken(String token) {
    this.token = token;
  }
}
