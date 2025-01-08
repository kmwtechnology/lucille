package com.kmwllc.lucille;

import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.auth.BasicAuthenticator;
import com.kmwllc.lucille.config.AuthConfiguration.AuthType;
import com.kmwllc.lucille.config.LucilleAPIConfiguration;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.endpoints.LivenessResource;
import com.kmwllc.lucille.endpoints.LucilleResource;
import com.kmwllc.lucille.endpoints.ReadinessResource;
import io.dropwizard.auth.AuthDynamicFeature;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.core.Application;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import jdk.jfr.Experimental;

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
  public void initialize(Bootstrap<LucilleAPIConfiguration> bootstrap) {

    bootstrap.addBundle(new SwaggerBundle<LucilleAPIConfiguration>() {
      @Override
      protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          LucilleAPIConfiguration configuration) {
        return configuration.swaggerBundleConfiguration;
      }
    });

  }

  @Override
  public void run(LucilleAPIConfiguration config, Environment env) throws Exception {
    System.out.println(String.format("starting lucille-api from %s config %s env %s",
        System.getProperty("user.dir"), config, env));

    RunnerManager runnerManager = RunnerManager.getInstance();

    boolean authEnabled = config.getAuthConfig().isEnabled();

    // Enable Basic Auth only if it's enabled in the configuration
    if (authEnabled) {
      if (config.getAuthConfig().getType().equals(AuthType.BASIC_AUTH)) {
        env.jersey()
            .register(new AuthDynamicFeature(new BasicCredentialAuthFilter.Builder<PrincipalImpl>()
                .setAuthenticator(new BasicAuthenticator(config.getAuthConfig().getPassword()))
                .buildAuthFilter()));
        env.jersey().register(new AuthValueFactoryProvider.Binder<>(PrincipalImpl.class));
        log.info("Basic authentication has been enabled.");
      } else {
        throw new Exception("Unsupported auth type configured for the Lucille Admin API.");
      }
    } else {
      log.info("Authentication is disabled.");
    }

    // Register our 3 Resources
    AuthHandler authHandler = new AuthHandler(authEnabled);
    env.jersey().register(new LucilleResource(runnerManager, authHandler));
    env.jersey().register(new LivenessResource());
    env.jersey().register(new ReadinessResource());
    env.jersey().register(new AuthValueFactoryProvider.Binder<>(PrincipalImpl.class));
  }

  public static void main(String[] args) throws Exception {
    System.out.println(String.format("starting lucille-api from %s args %s",
        System.getProperty("user.dir"), Arrays.toString(args)));

    // Use default config if no arguments are provided
    if (args.length == 0) {
      args = new String[] {"server", "conf/default-config.yml"}; // Specify the default config file
      System.out.println("No config file supplied. Using default: default-config.yml");
    }
    new APIApplication().run(args);
  }
}
