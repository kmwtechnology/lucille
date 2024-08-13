package com.kmwllc.lucille;

import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LivenessResource;
import com.kmwllc.lucille.endpoints.LucilleResource;
import com.kmwllc.lucille.endpoints.ReadinessResource;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import jdk.jfr.Experimental;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main Class for the Lucille API.
 */
@Experimental
public class APIApplication extends Application<LucilleAPIConfiguration> {

  public static final Logger log = LoggerFactory.getLogger(APIApplication.class);

  @Override
  public String getName() {
    return "lucille-api";
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
    env.jersey().register(new LucilleResource(runnerManager));
    env.jersey().register(new LivenessResource());
    env.jersey().register(new ReadinessResource());
  }

  public static void main(String[] args) throws Exception {
    new APIApplication().run(args);
  }
}
