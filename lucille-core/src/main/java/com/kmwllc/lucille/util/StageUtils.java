package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.StageException;

import java.util.List;

/**
 * Util class for the Stage API. Contains static methods to facilitate validation and other
 * operations common across Stages.
 */
public class StageUtils {

  /**
   * Validate that the given field list contains at least 1 field name.
   *
   * @param fields the field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumNotZero(List<String> fields, String stageName)
      throws StageException {
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
  public static void validateFieldNumsEqual(
      List<String> fields1, List<String> fields2, String stageName) throws StageException {
    if (fields1.size() != fields2.size()) {
      throw new StageException("Unequal length field lists supplied to " + stageName);
    }
  }

  /**
   * Validate that if the field lists have unequal numbers of field names, one of the field lists
   * contains 1 and only one field name.
   *
   * @param fields1 the first field list
   * @param fields2 the second field list
   * @param stageName the name of the calling stage
   * @throws StageException
   */
  public static void validateFieldNumsSeveralToOne(
      List<String> fields1, List<String> fields2, String stageName) throws StageException {
    if ((fields1.size() != fields2.size()) && (fields2.size() != 1)) {
      throw new StageException(
          stageName + " was supplied with an invalid number of fields in the inputted field lists");
    }
  }
}
