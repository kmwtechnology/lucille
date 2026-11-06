package com.kmwllc.lucille.api.endpoints;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.api.objects.AuthInfoResponse;
import jakarta.ws.rs.core.Response;
import org.junit.Test;

public class AuthInfoResourceTest {

  @Test
  public void testReportsAuthEnabled() {
    Response response = new AuthInfoResource(true).authInfo();

    assertEquals(200, response.getStatus());
    assertTrue(((AuthInfoResponse) response.getEntity()).isAuthEnabled());
  }

  @Test
  public void testReportsAuthDisabled() {
    Response response = new AuthInfoResource(false).authInfo();

    assertEquals(200, response.getStatus());
    assertFalse(((AuthInfoResponse) response.getEntity()).isAuthEnabled());
  }
}
