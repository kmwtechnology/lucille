package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigAccessor {

  public static final String ENV_PROP = "pipeline.env";

  private static final Logger log = LoggerFactory.getLogger(ConfigAccessor.class);

  public static Config loadConfig() throws ConfigException {

    String envName = System.getProperty(ENV_PROP);

    Config originalConfig = ConfigFactory.load();

    if (envName==null) {
      log.info("pipeline.env system property not set");
      return originalConfig;
    }

    log.info("pipeline.env system property = " + envName);
    // Override default settings in application.conf with settings for the named environment.
    // For example, if application.conf contains x=1 and dev.x=2, and if the the
    // pipeline.env system property is set to "dev", then accessing "x" will return 2
    // even though the "dev" prefix was not included
    return originalConfig.getConfig(envName).withFallback(originalConfig);

  }

}
