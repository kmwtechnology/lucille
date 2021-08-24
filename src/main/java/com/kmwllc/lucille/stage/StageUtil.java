package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;

/**
 * Util class for the Stage API. Contains static methods to facilitate validation and other operations common across
 * Stages.
 */
public class StageUtil {

  /**
   * Get the value of the given setting from the config file, or a default value if the setting does not exist in the
   * config.
   *
   * @param config  the config to search for the setting
   * @param setting the setting to get the value of
   * @param fallback  default value
   * @param <T> the Type of this setting's value
   * @return  the value
   */
  public static <T> T configGetOrDefault(Config config, String setting, T fallback) {
    if (config.hasPath(setting)) {
      return (T) config.getValue(setting).unwrapped();
    }

    return fallback;
  }

  /**
   * Validate that the given field list contains at least 1 field name.
   *
   * @param fieldStr  the field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumNotZero(String fieldStr, String stageName) throws StageException {
    String[] fields = fieldStr.split(",");

    if (fields.length == 0) {
      throw new StageException("An empty field list was supplied to " + stageName);
    }
  }

  /**
   * Validate that the two given field lists contain the same number of field names.
   *
   * @param fieldStr1 the first field list
   * @param fieldStr2 the second field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumsEqual(String fieldStr1, String fieldStr2, String stageName) throws StageException{
    String[] fields1 = fieldStr1.split(",");
    String[] fields2 = fieldStr2.split(",");

    if (fields1.length != fields2.length) {
      throw new StageException("Unequal length field lists supplied to " + stageName);
    }
  }

  /**
   * Validate that if the field lists have unequal numbers of field names, one of the field lists contains 1 and
   * only one field name.
   *
   * @param fieldStr1 the first field list
   * @param fieldStr2 the second field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumsOneToSeveral(String fieldStr1, String fieldStr2, String stageName) throws StageException {
    String[] fields1 = fieldStr1.split(",");
    String[] fields2 = fieldStr2.split(",");

    if ((fields1.length != fields2.length) && (fields1.length != 1) && (fields2.length != 1)) {
      throw new StageException(stageName + " was supplied with an invalid number of fields in the inputted field lists");
    }
  }
}
