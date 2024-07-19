package com.kmwllc.lucille;

import com.kmwllc.lucille.auth.BasicAuthenticator;
import com.kmwllc.lucille.config.AuthConfiguration.AuthType;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import com.kmwllc.lucille.endpoints.LucilleLivenessResource;
import com.kmwllc.lucille.endpoints.LucilleReadinessResource;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class for the Lucille Admin API.
 */
public class AdminAPI extends Application<LucilleAPIConfiguration> {

  public static final Logger log = LoggerFactory.getLogger(AdminAPI.class);

  @Override
  public String getName() {
    return "lucille-admin-api";
  }

  // Turn off the default Dropwizard Logging
  @Override
  protected void bootstrapLogging() {}

  @Override
  public void initialize(Bootstrap<LucilleAPIConfiguration> bootstrap) {}

  @Override
  public void run(LucilleAPIConfiguration config, Environment env) throws Exception {
    RunnerManager runnerManager = RunnerManager.getInstance();

    // Enable Basic Auth
    if (config.getAuthConfig().getType().equals(AuthType.BASIC_AUTH)) {
      env.jersey().register(new AuthDynamicFeature(
          new BasicCredentialAuthFilter.Builder<PrincipalImpl>()
              .setAuthenticator(new BasicAuthenticator(config.getAuthConfig().getPassword()))
              .buildAuthFilter()
      ));
    } else {
      throw new Exception("No Auth configured for the Lucille Admin API.");
    }

    // Register our 3 Resources
    env.jersey().register(new LucilleAdminResource(runnerManager));
    env.jersey().register(new LucilleLivenessResource());
    env.jersey().register(new LucilleReadinessResource());
    env.jersey().register(new AuthValueFactoryProvider.Binder<>(PrincipalImpl.class));
  }

  public static void main(String[] args) throws Exception {
    new AdminAPI().run(args);
  }
}
