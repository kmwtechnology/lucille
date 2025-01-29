package com.kmwllc.lucille.config;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.dropwizard.core.Configuration;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;

// Empty Custom Configuration Class for if we want to extend the Dropwizard Configuration moving forward
// https://www.dropwizard.io/en/stable/manual/configuration.html#man-configuration
public class LucilleAPIConfiguration extends Configuration {

  @Valid
  @NotNull
  private AuthConfiguration authConfig;

  @Valid
  @NotNull
  public SwaggerBundleConfiguration swaggerBundleConfiguration;

  @JsonProperty("swagger")
  public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
	  return swaggerBundleConfiguration;
  }
  
  @JsonProperty("swagger")
  public void setSwaggerBundleConfiguration(SwaggerBundleConfiguration swaggerBundleConfiguration) {
	  this.swaggerBundleConfiguration = swaggerBundleConfiguration;
  }
  
  @JsonProperty("auth")
  public AuthConfiguration getAuthConfig() {
    return this.authConfig;
  }

  @JsonProperty("auth")
  public void setAuthConfig(AuthConfiguration authConfig) {
    this.authConfig = authConfig;
  }

}