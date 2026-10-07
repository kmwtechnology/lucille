package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigValueType;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.http.Header;
import org.apache.http.message.BasicHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility functions for working with Configs.
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

  /**
   * Reads a setting that may be either a single String or a list of Strings, always returning a list.
   *
   * @param config the config to read from
   * @param path the setting to read; throws if it is missing or is neither a String nor a list of Strings
   * @return the value as a list; a single String becomes a one-element list
   */
  public static List<String> getStringOrList(Config config, String path) {
    if (config.getValue(path).valueType() == ConfigValueType.LIST) {
      return config.getStringList(path);
    }
    return List.of(config.getString(path));
  }
}
