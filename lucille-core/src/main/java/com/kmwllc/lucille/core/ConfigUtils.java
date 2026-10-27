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
   * <p>The value is read with the typed Config getter that matches the fallback's type (getInt for an Integer fallback,
   * getBoolean for a Boolean fallback, and so on), so HOCON's usual coercions apply: an integer literal is accepted for a
   * Long or Double setting, and a numeric or boolean string (for example from an environment variable substitution) is
   * accepted for a number or boolean setting. A value that cannot be coerced fails with a ConfigException naming the
   * setting, instead of a ClassCastException at the point of use.
   *
   * <p>A {@code null} fallback carries no type information, so the raw unwrapped value is returned. Prefer
   * {@link #getStringOrDefault}, {@link #getIntOrDefault}, {@link #getLongOrDefault}, {@link #getDoubleOrDefault} or
   * {@link #getBooleanOrDefault} when the default is null.
   *
   * @param config  the config to search for the setting
   * @param setting the setting to get the value of
   * @param fallback  default value
   * @param <T> the Type of this setting's value
   * @return the value
   */
  @SuppressWarnings("unchecked")
  public static <T> T getOrDefault(Config config, String setting, T fallback) {
    if (!config.hasPath(setting)) {
      return fallback;
    }
    Object value;
    if (fallback instanceof Boolean) {
      value = config.getBoolean(setting);
    } else if (fallback instanceof Integer) {
      value = config.getInt(setting);
    } else if (fallback instanceof Long) {
      value = config.getLong(setting);
    } else if (fallback instanceof Double) {
      value = config.getDouble(setting);
    } else if (fallback instanceof Float) {
      value = (float) config.getDouble(setting);
    } else if (fallback instanceof Number) {
      value = config.getNumber(setting);
    } else if (fallback instanceof String) {
      value = config.getString(setting);
    } else if (fallback instanceof List) {
      value = config.getAnyRefList(setting);
    } else {
      value = config.getValue(setting).unwrapped();
    }
    return (T) value;
  }

  /** Returns the setting as a String (coercing numbers and booleans), or the fallback if it is absent. */
  public static String getStringOrDefault(Config config, String setting, String fallback) {
    return config.hasPath(setting) ? config.getString(setting) : fallback;
  }

  /** Returns the setting as an Integer (coercing numeric strings), or the fallback if it is absent. */
  public static Integer getIntOrDefault(Config config, String setting, Integer fallback) {
    // not a ternary: a ternary would unbox a null fallback and throw NullPointerException
    if (config.hasPath(setting)) {
      return config.getInt(setting);
    }
    return fallback;
  }

  /** Returns the setting as a Long (coercing integer literals and numeric strings), or the fallback if it is absent. */
  public static Long getLongOrDefault(Config config, String setting, Long fallback) {
    // not a ternary: a ternary would unbox a null fallback and throw NullPointerException
    if (config.hasPath(setting)) {
      return config.getLong(setting);
    }
    return fallback;
  }

  /** Returns the setting as a Double (coercing integer literals and numeric strings), or the fallback if it is absent. */
  public static Double getDoubleOrDefault(Config config, String setting, Double fallback) {
    // not a ternary: a ternary would unbox a null fallback and throw NullPointerException
    if (config.hasPath(setting)) {
      return config.getDouble(setting);
    }
    return fallback;
  }

  /** Returns the setting as a Boolean (coercing "true"/"false" strings), or the fallback if it is absent. */
  public static Boolean getBooleanOrDefault(Config config, String setting, Boolean fallback) {
    // not a ternary: a ternary would unbox a null fallback and throw NullPointerException
    if (config.hasPath(setting)) {
      return config.getBoolean(setting);
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
