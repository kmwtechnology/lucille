package com.kmwllc.lucille.api;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.kmwllc.lucille.api.auth.BasicAuthenticator;
import com.kmwllc.lucille.api.auth.JwtAuthenticator;
import com.kmwllc.lucille.api.auth.JwtService;
import com.kmwllc.lucille.api.auth.RequireAuthDynamicFeature;
import com.kmwllc.lucille.api.config.AuthConfiguration.AuthType;
import com.kmwllc.lucille.api.config.LucilleAPIConfiguration;
import com.kmwllc.lucille.core.RunnerManager;
import com.kmwllc.lucille.api.endpoints.AuthInfoResource;
import com.kmwllc.lucille.api.endpoints.ConfigInfo;
import com.kmwllc.lucille.api.endpoints.LivenessResource;
import com.kmwllc.lucille.api.endpoints.LoginResource;
import com.kmwllc.lucille.api.endpoints.LucilleResource;
import com.kmwllc.lucille.api.endpoints.ReadinessResource;
import com.kmwllc.lucille.api.endpoints.SystemStatsResource;
import com.kmwllc.lucille.ui.AdminUiServlet;
import io.dropwizard.auth.AuthFilter;
import io.dropwizard.auth.AuthValueFactoryProvider;
import io.dropwizard.auth.PrincipalImpl;
import io.dropwizard.auth.basic.BasicCredentialAuthFilter;
import io.dropwizard.auth.chained.ChainedAuthFilter;
import io.dropwizard.auth.oauth.OAuthCredentialAuthFilter;
import io.dropwizard.configuration.EnvironmentVariableSubstitutor;
import io.dropwizard.configuration.SubstitutingSourceProvider;
import io.dropwizard.core.Application;
import io.dropwizard.core.server.DefaultServerFactory;
import io.dropwizard.core.server.ServerFactory;
import io.dropwizard.core.server.SimpleServerFactory;
import io.dropwizard.core.setup.Bootstrap;
import io.dropwizard.core.setup.Environment;
import io.dropwizard.jetty.ConnectorFactory;
import io.dropwizard.jetty.HttpsConnectorFactory;
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
 * @see com.kmwllc.lucille.api.config.LucilleAPIConfiguration
 */
public class APIApplication extends Application<LucilleAPIConfiguration> {

  // The API logs through log4j2 with Lucille's default config, like the rest of Lucille. Logback is also on the classpath,
  // because Dropwizard's classes need it, so log4j2 is selected explicitly unless the caller chose a provider or config.
  // This runs before the logger below is created, which is what initializes SLF4J.
  static {
    if (System.getProperty("slf4j.provider") == null) {
      System.setProperty("slf4j.provider", "org.apache.logging.slf4j.SLF4JServiceProvider");
    }
    if (System.getProperty("log4j2.configurationFile") == null) {
      System.setProperty("log4j2.configurationFile", "classpath:lucille-log4j2.xml");
    }
  }

  /**
   * Logger for the Lucille API application.
   */
  public static final Logger log = LoggerFactory.getLogger(APIApplication.class);

  /**
   * Packages whose resources are reachable without authentication. These hold the resources that serve the
   * Swagger UI and the OpenAPI document, which are declared outside this codebase and so cannot be annotated
   * with {@link com.kmwllc.lucille.api.auth.AuthNotRequired}.
   */
  private static final Set<String> AUTH_EXEMPT_PACKAGES =
      Set.of("io.federecio.dropwizard.swagger", "io.swagger.v3.jaxrs2");

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
   * Disables the default Dropwizard logging configuration so that Log4j2 can take over.
   */
  @Override
  protected void bootstrapLogging() {}

  /**
   * Initializes the Dropwizard application, enabling environment-variable substitution in the
   * configuration file and registering the Swagger bundle. Substitution lets the config reference
   * values such as {@code ${LUCILLE_API_PASSWORD}} that are injected at runtime rather than
   * committed to the file; undefined variables resolve to their {@code :-} default (or empty)
   * rather than failing startup.
   * @param bootstrap the Dropwizard bootstrap object
   */
  @Override
  public void initialize(Bootstrap<LucilleAPIConfiguration> bootstrap) {
    bootstrap.setConfigurationSourceProvider(
        new SubstitutingSourceProvider(
            bootstrap.getConfigurationSourceProvider(),
            new EnvironmentVariableSubstitutor(false)));

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
   * @throws IllegalArgumentException if authentication is enabled and AuthType is NO_AUTH, or the password is missing or blank
   * @throws Exception for other startup failures
   */
  @Override
  public void run(LucilleAPIConfiguration config, Environment env) throws Exception {
    log.info("starting lucille-api from {} config {} env {}", System.getProperty("user.dir"), config, env);

    RunnerManager runnerManager = RunnerManager.getInstance();

    // shared by the login endpoint and the bearer filter so they use the same key
    JwtService jwtService = new JwtService();

    boolean authEnabled = config.getAuthConfig().isEnabled();

    if (authEnabled) {
      AuthType authType = config.getAuthConfig().getType();

      if (AuthType.NO_AUTH.equals(authType)) {
        throw new IllegalArgumentException("auth.type must be set to basicAndBearer when auth.enabled is true.");
      }

      String password = config.getAuthConfig().getPassword();
      if (password == null || password.isBlank()) {
        throw new IllegalArgumentException("auth.password must be set and not blank when auth.enabled is true.");
      }

      env.jersey().register(new RequireAuthDynamicFeature(buildAuthFilter(config, jwtService), AUTH_EXEMPT_PACKAGES));
      env.jersey().register(new AuthValueFactoryProvider.Binder<>(PrincipalImpl.class));
      log.info("Authentication has been enabled (Basic and Bearer accepted).");
      if (usesPlainHttp(config)) {
        log.warn("Authentication is enabled but the API is serving plain HTTP, so passwords and bearer tokens are sent "
            + "unencrypted. Configure an https application connector, or terminate TLS in front of the API.");
      }
    } else {
      log.info("Authentication is disabled.");
    }

    PresetConfigHandler presetConfigHandler;
    if (config.getPresetConfigConfiguration() != null) {
      presetConfigHandler = new PresetConfigHandler(config.getPresetConfigConfiguration().getConfigDirectoryPath());
    } else {
      presetConfigHandler = new PresetConfigHandler(null);
    }

    // may throw an exception for IO errors
    Map<String, Config> presetConfigs = presetConfigHandler.fetchConfigs();

    // Register our Resources
    env.jersey().register(new LucilleResource(runnerManager, config.isPreventConcurrentRuns(), presetConfigs));
    env.jersey().register(new LivenessResource());
    env.jersey().register(new ReadinessResource());

    // Register SystemStatsResource
    env.jersey().register(new SystemStatsResource());
    env.jersey().register(new ConfigInfo());

    env.jersey().register(new LoginResource(jwtService, config.getAuthConfig().getPassword()));
    env.jersey().register(new AuthInfoResource(authEnabled));

    registerAdminUi(env);
  }

  /**
   * Whether any application connector serves plain HTTP. Admin connectors are ignored, and TLS that is
   * terminated in front of the API (for example by a reverse proxy) cannot be detected here.
   * @param config the application configuration
   * @return true if at least one application connector is not HTTPS
   */
  private static boolean usesPlainHttp(LucilleAPIConfiguration config) {
    ServerFactory serverFactory = config.getServerFactory();
    List<ConnectorFactory> connectors;
    if (serverFactory instanceof DefaultServerFactory defaultFactory) {
      connectors = defaultFactory.getApplicationConnectors();
    } else if (serverFactory instanceof SimpleServerFactory simpleFactory) {
      connectors = List.of(simpleFactory.getConnector());
    } else {
      return false;
    }
    return connectors.stream().anyMatch(connector -> !(connector instanceof HttpsConnectorFactory));
  }

  /**
   * Builds the authentication filter applied to protected resources: a chain accepting either HTTP
   * Basic credentials or a Bearer token.
   * @param config the application configuration
   * @param jwtService the service used to verify bearer tokens
   * @return a chained Basic-or-Bearer authentication filter
   */
  private static AuthFilter<?, PrincipalImpl> buildAuthFilter(LucilleAPIConfiguration config, JwtService jwtService) {
    AuthFilter<?, PrincipalImpl> basicFilter = new BasicCredentialAuthFilter.Builder<PrincipalImpl>()
        .setAuthenticator(new BasicAuthenticator(config.getAuthConfig().getPassword()))
        .buildAuthFilter();

    AuthFilter<?, PrincipalImpl> bearerFilter = new OAuthCredentialAuthFilter.Builder<PrincipalImpl>()
        .setAuthenticator(new JwtAuthenticator(jwtService))
        .setPrefix("Bearer")
        .buildAuthFilter();

    // ChainedAuthFilter takes a raw List<AuthFilter> and tries each until one authenticates
    @SuppressWarnings({"unchecked", "rawtypes"})
    AuthFilter<?, PrincipalImpl> chained = new ChainedAuthFilter(List.of(basicFilter, bearerFilter));
    return chained;
  }

  /**
   * Registers the admin UI servlet at {@code /admin/*} when the built UI is on the classpath;
   * @param env the Dropwizard environment
   */
  private void registerAdminUi(Environment env) {
    boolean uiBundled = Thread.currentThread().getContextClassLoader()
        .getResource("admin/index.html") != null
        || APIApplication.class.getClassLoader().getResource("admin/index.html") != null;

    if (!uiBundled) {
      log.info("Admin UI assets not found on the classpath; /admin will not be served. "
          + "Add lucille-admin-ui.jar to the classpath to enable it.");
      return;
    }

    env.servlets()
        .addServlet("admin-ui", new AdminUiServlet())
        .addMapping("/admin", "/admin/*");
    log.info("Admin UI is being served at /admin (unauthenticated).");
  }

  /**
   * Main method for launching the Lucille API server. If no args are provided, defaults to 'server conf/default-config.yml'.
   * @param args command-line arguments (expects Dropwizard {@code server <config.yml>})
   * @throws Exception if startup fails
   */
  public static void main(String[] args) throws Exception {
    log.info("starting lucille-api from {} args {}", System.getProperty("user.dir"), Arrays.toString(args));

    // Use default config if no arguments are provided
    if (args.length == 0) {
      args = new String[] {"server", "conf/default-config.yml"}; // Specify the default config file
      log.info("No config file supplied. Using default: default-config.yml");
    }
    new APIApplication().run(args);
  }
}
