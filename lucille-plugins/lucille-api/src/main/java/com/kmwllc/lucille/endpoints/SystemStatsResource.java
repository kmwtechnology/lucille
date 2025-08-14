package com.kmwllc.lucille.endpoints;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.util.LogUtils;
import jakarta.annotation.security.PermitAll;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.core.Response.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import oshi.SystemInfo;
import oshi.hardware.HardwareAbstractionLayer;
import oshi.hardware.CentralProcessor;
import oshi.hardware.GlobalMemory;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

/**
 * Exposes OS and JVM resource usage via a REST endpoint.
 * Returns CPU, RAM, JVM heap, and disk metrics as JSON.
 *
 * JSON schema:
 * <pre>
 * {
 *   "cpu": { "percent": double, "used": double, "available": double, "total": 100, "loadAverage": double },
 *   "ram": { "total": long, "available": long, "used": long, "percent": double },
 *   "jvm": { "total": long, "free": long, "used": long, "percent": double },
 *   "storage": { "total": long, "available": long, "used": long, "percent": double }
 * }
 * </pre>
 */
@Path("/v1/systemstats")
@Tag(name = "System", description = "System statistics info.")
@Produces(MediaType.APPLICATION_JSON)
@PermitAll
public class SystemStatsResource {

  private static final Logger log = LoggerFactory.getLogger(SystemStatsResource.class);
  /** CPU sampling interval in milliseconds. */
  private static final long CPU_SAMPLE_INTERVAL_MS = 100;
  /** Factor for rounding percentage values to two decimals. */
  private static final double PERCENT_ROUND_FACTOR = 100.0;

  public SystemStatsResource() {
    super();
  }

  @GET
  @Operation(summary = "System Statistics", description = "Returns CPU, JVM memory, and physical RAM usage as percent.")
  /**
   * Returns system statistics: CPU load, RAM, JVM heap, and storage usage.
   * Samples CPU load over {@link #CPU_SAMPLE_INTERVAL_MS} ms and rounds values to two decimals.
   * InterruptedException during sleep is ignored.
   * @return Response with JSON map of system metrics.
   */
  public Response getSystemStats() {
    log.debug("System stats endpoint accessed.");
    SystemInfo si = new SystemInfo();
    HardwareAbstractionLayer hal = si.getHardware();
    CentralProcessor cpu = hal.getProcessor();
    long[] prevTicks = cpu.getSystemCpuLoadTicks();
    try { Thread.sleep(CPU_SAMPLE_INTERVAL_MS); } catch (InterruptedException ignored) {}
    double cpuLoad = cpu.getSystemCpuLoadBetweenTicks(prevTicks) * 100;
    GlobalMemory mem = hal.getMemory();
    long totalPhysical = mem.getTotal();
    long freePhysical = mem.getAvailable();
    double ramPercent = ((double)(totalPhysical - freePhysical) / totalPhysical) * 100;
    File root = new File("/");
    long totalDisk = root.getTotalSpace();
    long freeDisk = root.getFreeSpace();
    double storagePercent = ((double)(totalDisk - freeDisk) / totalDisk) * 100;
    Runtime runtime = Runtime.getRuntime();
    long totalJvm = runtime.totalMemory();
    long freeJvm = runtime.freeMemory();
    double jvmPercent = ((double)(totalJvm - freeJvm) / totalJvm) * 100;
    Map<String,Object> cpuStats = new HashMap<>();
    double cpuPercent = Math.round(cpuLoad * PERCENT_ROUND_FACTOR) / PERCENT_ROUND_FACTOR;
    cpuStats.put("percent", cpuPercent);
    cpuStats.put("used", cpuPercent);
    cpuStats.put("available", Math.round((100 - cpuPercent) * PERCENT_ROUND_FACTOR) / PERCENT_ROUND_FACTOR);
    cpuStats.put("total", 100);
    cpuStats.put("loadAverage", cpu.getSystemLoadAverage(1)[0]);
    Map<String,Object> ramStats = new HashMap<>();
    ramStats.put("total", totalPhysical);
    ramStats.put("available", freePhysical);
    long ramUsed = totalPhysical - freePhysical;
    ramStats.put("used", ramUsed);
    double ramPct = Math.round(ramPercent * PERCENT_ROUND_FACTOR) / PERCENT_ROUND_FACTOR;
    ramStats.put("percent", ramPct);
    Map<String,Object> jvmStats = new HashMap<>();
    jvmStats.put("total", totalJvm);
    jvmStats.put("free", freeJvm);
    long jvmUsed = totalJvm - freeJvm;
    jvmStats.put("used", jvmUsed);
    double jvmPct = Math.round(jvmPercent * PERCENT_ROUND_FACTOR) / PERCENT_ROUND_FACTOR;
    jvmStats.put("percent", jvmPct);
    Map<String,Object> storageStats = new HashMap<>();
    storageStats.put("total", totalDisk);
    storageStats.put("available", freeDisk);
    long storageUsed = totalDisk - freeDisk;
    storageStats.put("used", storageUsed);
    double storagePct = Math.round(storagePercent * PERCENT_ROUND_FACTOR) / PERCENT_ROUND_FACTOR;
    storageStats.put("percent", storagePct);
    Map<String,Object> stats = new HashMap<>();
    stats.put("cpu", cpuStats);
    stats.put("ram", ramStats);
    stats.put("jvm", jvmStats);
    stats.put("storage", storageStats);
    return Response.ok(stats).build();
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
