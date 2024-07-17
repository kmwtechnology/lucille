package com.kmwllc.lucille;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LucilleAdminResource;
import com.kmwllc.lucille.endpoints.LucilleLivenessResource;
import com.kmwllc.lucille.endpoints.LucilleReadinessResource;
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

    // Register our 3 Resources
    env.jersey().register(new LucilleAdminResource(runnerManager));
    env.jersey().register(new LucilleLivenessResource());
    env.jersey().register(new LucilleReadinessResource());
  }

  public static void main(String[] args) throws Exception {
    new AdminAPI().run(args);
  }
}
