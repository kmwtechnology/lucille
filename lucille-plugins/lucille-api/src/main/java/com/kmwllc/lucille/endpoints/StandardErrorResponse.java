package com.kmwllc.lucille.endpoints;

import java.util.HashMap;
import java.util.Map;
import jakarta.ws.rs.core.Response;

public class StandardErrorResponse {

  private final int status;
  private final String message;

  public StandardErrorResponse(int status, String message) {
    this.status = status;
    this.message = message;
  }

  public int getStatus() {
    return status;
  }

  public String getMessage() {
    return message;
  }

  public static Response buildResponse(Response.Status status, String message) {
    Map<String, Object> errorBody = new HashMap<>();
    errorBody.put("status", status.getStatusCode());
    errorBody.put("error", status.getReasonPhrase());
    errorBody.put("message", message);

    return Response.status(status).entity(errorBody).type("application/json").build();
  }
}
