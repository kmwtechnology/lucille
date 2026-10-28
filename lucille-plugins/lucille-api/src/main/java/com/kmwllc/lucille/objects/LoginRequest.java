package com.kmwllc.lucille.objects;

/**
 * A request to authenticate against the Lucille API and obtain a bearer token.
 */
public class LoginRequest {
  private String password;

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }
}
