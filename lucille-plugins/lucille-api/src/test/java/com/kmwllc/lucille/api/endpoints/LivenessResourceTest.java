package com.kmwllc.lucille.api.endpoints;

import static org.junit.Assert.assertEquals;
import org.junit.Test;
import jakarta.ws.rs.core.Response;

public class LivenessResourceTest {

  private static final LivenessResource liveness = new LivenessResource();

  @Test
  public void testLivenessEndpoint() {
    Response response = liveness.isAlive();      
    assertEquals(Response.ok().build().toString(), response.toString());
  }

}
