package com.kmwllc.lucille.endpoints;

import java.util.HashMap;
import java.util.Map;
import jakarta.ws.rs.core.Response;

/**
 * Utility class for building standard error responses for Lucille API endpoints.
 * <p>
 * Provides helper methods to create consistent JSON error responses for API clients.
 */
public class ResponseUtils {

  /**
   * Private constructor to prevent instantiation.
   */
  private ResponseUtils() {}

  /**
   * Builds a standardized JSON error response for the API.
   *
   * @param status the HTTP response status
   * @param message the error message to include in the response
   * @return a Response object with the error details and status
   */
  public static Response buildErrorResponse(Response.Status status, String message) {
    Map<String, Object> errorBody = new HashMap<>();
    errorBody.put("status", status.getStatusCode());
    errorBody.put("error", status.getReasonPhrase());
    errorBody.put("message", message);

    return Response.status(status).entity(errorBody).type("application/json").build();
  }
}
