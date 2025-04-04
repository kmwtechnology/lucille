package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for working with Config.
 */
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

  /**
   * Creates an array of org.apache.http.Headers that can be used for http requests, given a config that contains a header mapping field.
   * If the field doesn't exist, returns null.
   *
   * @param config the config to get the header text from
   * @param name field in the config that we'll get the header data from
   * @return the array of Headers
   */
  public static Header[] createHeaderArray(Config config, String name) {
    if (!config.hasPath(name)) {
      return null;
    }
    List<Header> headerList = new ArrayList<>();
    for (Map.Entry<String, Object> entry : config.getConfig(name).root().unwrapped().entrySet()) {
      headerList.add(new BasicHeader(entry.getKey(), (String) entry.getValue()));
    }
    return headerList.toArray(new Header[0]);
  }
}
