package com.kmwllc.lucille.endpoints;

import java.util.HashMap;
import java.util.Map;
import jakarta.ws.rs.core.Response;

public class ResponseUtils {

  private ResponseUtils() {}

  public static Response buildErrorResponse(Response.Status status, String message) {
    Map<String, Object> errorBody = new HashMap<>();
    errorBody.put("status", status.getStatusCode());
    errorBody.put("error", status.getReasonPhrase());
    errorBody.put("message", message);

    return Response.status(status).entity(errorBody).type("application/json").build();
  }
}
