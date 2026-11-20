package com.kmwllc.lucille.api.endpoints;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.util.LogUtils;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;

/**
 * Exposes Lucille's Dropwizard metrics registry via a REST endpoint.
 */
@Path("/v1/systemstats")
@Tag(name = "System", description = "System statistics info.")
@Produces(MediaType.APPLICATION_JSON)
public class SystemStatsResource {

  public SystemStatsResource() {
    super();
  }

  @GET
  @Path("/metrics")
  @Operation(summary = "Dropwizard metrics", description = "Returns the Dropwizard metrics registry as JSON.")
  @Produces(MediaType.APPLICATION_JSON)
  /**
   * Returns the Dropwizard metrics registry as JSON.
   * Serializes the {@link MetricRegistry} using Jackson's {@link MetricsModule}.
   * @return Response with JSON map of the metrics registry.
   */
  public Response getDropwizardMetrics() {
    MetricRegistry registry = SharedMetricRegistries.getOrCreate(LogUtils.METRICS_REG);

    ObjectMapper mapper = new ObjectMapper()
        .registerModule(new MetricsModule(SECONDS, MILLISECONDS, false
        ));

    try {
      return Response.ok(mapper.valueToTree(registry)).build();
    } catch (Exception e) {
      return ResponseUtils.buildErrorResponse(Status.INTERNAL_SERVER_ERROR, "Failed to serialize Dropwizard metrics.");
    }
  }
}
