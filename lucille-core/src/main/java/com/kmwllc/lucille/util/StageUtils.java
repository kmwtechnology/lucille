package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.StageException;

import java.util.List;
import java.util.Map;
import org.apache.zookeeper.common.StringUtils;

/**
 * Util class for the Stage API. Contains static methods to facilitate validation and other operations common across
 * Stages.
 */
public class StageUtils {

  /**
   * Validate that the given field list contains at least 1 field name.
   *
   * @param fields  the field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static <T> void validateListLenNotZero(List<T> fields, String stageName) throws StageException {
    if (fields.isEmpty()) {
      throw new StageException("An empty field list was supplied to " + stageName);
    }
  }

  /**
   * Validate that the two given field lists contain the same number of field names.
   *
   * @param fields1 the first field list
   * @param fields2 the second field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static <T> void validateFieldNumsEqual(List<T> fields1, List<T> fields2, String stageName) throws StageException {
    if (fields1.size() != fields2.size()) {
      throw new StageException("Unequal length field lists supplied to " + stageName);
    }
  }

  /**
   * Validate that if the field lists have unequal numbers of field names, the second field list contains 1 and
   * only one field name.
   *
   * @param fields1 the first field list
   * @param fields2 the second field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumsSeveralToOne(List<String> fields1, List<String> fields2, String stageName)
      throws StageException {
    if ((fields1.size() != fields2.size()) && (fields2.size() != 1)) {
      throw new StageException(stageName + " was supplied with an invalid number of fields in the inputted field lists");
    }
  }

  public static void validateExpectedSize(Integer size, int expectedSize, String stageName, String message) throws StageException {
    if (size != expectedSize) {
      throw new StageException(stageName + ": " + message);
    }
  }

  public static void validateLessThan(Integer size, int lessThan, String stageName, String message) throws StageException {
    if (size >= lessThan) {
      throw new StageException(stageName + ": " + message);
    }
  }

  public static void validateMoreThan(Integer size, int moreThan, String stageName, String message) throws StageException {
    if (size <= moreThan) {
      throw new StageException(stageName + ": " + message);
    }
  }

  public static void validateStringNotEmpty(String str, String stageName, String message) throws StageException {
    if (StringUtils.isBlank(str)) {
      throw new StageException(stageName + ": " + message);
    }
  }

  public static <K, V> void validateNonEmptyMap(Map<K, V> map, String stageName, String message) throws StageException {
    if (map.isEmpty()) {
      throw new StageException(stageName + ": " + message);
    }
  }
}
