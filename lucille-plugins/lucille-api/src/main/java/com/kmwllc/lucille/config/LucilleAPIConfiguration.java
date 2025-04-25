package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.core.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

/**
 * Custom configuration class for the Lucille API service.
 * <p>
 * Extends Dropwizard's {@link Configuration} to allow for additional configuration options,
 * such as authentication and Swagger documentation settings.
 *
 * @see <a href="https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration">Dropwizard Configuration Docs</a>
 */
public class LucilleAPIConfiguration extends Configuration {

  /**
   * Default constructor for LucilleAPIConfiguration.
   * Required for deserialization and Javadoc compliance.
   */
  public LucilleAPIConfiguration() {
    // No-op constructor
  }

  /**
   * Authentication configuration for the Lucille API.
   */
  @Valid
  @NotNull
  private AuthConfiguration authConfig;

  /**
   * Swagger/OpenAPI configuration for the Lucille API.
   */
  @Valid
  @NotNull
  public SwaggerBundleConfiguration swaggerBundleConfiguration;

  /**
   * Returns the Swagger/OpenAPI configuration.
   * @return the SwaggerBundleConfiguration
   */
  @JsonProperty("swagger")
  public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
    return swaggerBundleConfiguration;
  }

  /**
   * Sets the Swagger/OpenAPI configuration.
   * @param swaggerBundleConfiguration the SwaggerBundleConfiguration to set
   */
  @JsonProperty("swagger")
  public void setSwaggerBundleConfiguration(SwaggerBundleConfiguration swaggerBundleConfiguration) {
    this.swaggerBundleConfiguration = swaggerBundleConfiguration;
  }

  /**
   * Returns the authentication configuration.
   * @return the AuthConfiguration
   */
  @JsonProperty("auth")
  public AuthConfiguration getAuthConfig() {
    return this.authConfig;
  }

  /**
   * Sets the authentication configuration.
   * @param authConfig the AuthConfiguration to set
   */
  @JsonProperty("auth")
  public void setAuthConfig(AuthConfiguration authConfig) {
    this.authConfig = authConfig;
  }

}