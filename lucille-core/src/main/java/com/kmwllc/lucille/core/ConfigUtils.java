package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigException;
import com.typesafe.config.ConfigFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtils {

  public static final String ENV_PROP = "pipeline.env";

  private static final Logger log = LoggerFactory.getLogger(ConfigUtils.class);

  /**
   * Get the value of the given setting from the config file, or a default value if the setting does not exist in the
   * config.
   *
   * @param config  the config to search for the setting
   * @param setting the setting to get the value of
   * @param fallback  default value
   * @param <T> the Type of this setting's value
   * @return the value
   */
  public static <T> T getOrDefault(Config config, String setting, T fallback) {
    if (config.hasPath(setting)) {
      return (T) config.getValue(setting).unwrapped();
    }

    return fallback;
  }

}
