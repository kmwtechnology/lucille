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
 * Main entry point for the Lucille API.
 * <p>
 * Configures authentication, Swagger, and resource registration for the Dropwizard server.
 * <p>
 * Usage:
 * <pre>
 *   java -jar lucille-api.jar server conf/config.yml
 * </pre>
 */
@Experimental
public class APIApplication extends Application<LucilleAPIConfiguration> {

  /**
   * Logger for the Lucille API application.
   */
  public static final Logger log = LoggerFactory.getLogger(APIApplication.class);

  /**
   * Default constructor for APIApplication.
   * Required for Javadoc compliance.
   */
  public APIApplication() {
    // No-op constructor
  }

  /**
   * Returns the name of the application.
   * @return the application name
   */
  @Override
  public String getName() {
    return "lucille-api";
  }

  /**
   * Disables the default Dropwizard logging configuration.
   */
  @Override
  protected void bootstrapLogging() {}

  /**
   * Initializes the Dropwizard application, including Swagger bundle registration.
   * @param bootstrap the Dropwizard bootstrap object
   */
  @Override
  public void initialize(Bootstrap<LucilleAPIConfiguration> bootstrap) {
    bootstrap.addBundle(new SwaggerBundle<LucilleAPIConfiguration>() {
      /**
       * Returns the Swagger bundle configuration from the Lucille API configuration.
       * @param configuration the Lucille API configuration
       * @return the SwaggerBundleConfiguration
       */
      @Override
      protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(
          LucilleAPIConfiguration configuration) {
        return configuration.swaggerBundleConfiguration;
      }
    });
  }

  /**
   * Starts the Lucille API server, configures authentication, and registers resources.
   * @param config the Lucille API configuration
   * @param env the Dropwizard environment
   * @throws Exception if authentication type is unsupported or startup fails
   */
  @Override
  public void run(LucilleAPIConfiguration config, Environment env) throws Exception {
    log.info(String.format("starting lucille-api from %s config %s env %s",
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

  /**
   * Main method for launching the Lucille API server.
   * @param args command-line arguments (expects Dropwizard &quot;server &lt;config.yml&gt;&quot;)
   * @throws Exception if startup fails
   */
  public static void main(String[] args) throws Exception {
    log.info(String.format("starting lucille-api from %s args %s",
        System.getProperty("user.dir"), Arrays.toString(args)));

    // Use default config if no arguments are provided
    if (args.length == 0) {
      args = new String[] {"server", "conf/default-config.yml"}; // Specify the default config file
      log.info("No config file supplied. Using default: default-config.yml");
    }
    new APIApplication().run(args);
  }
}
