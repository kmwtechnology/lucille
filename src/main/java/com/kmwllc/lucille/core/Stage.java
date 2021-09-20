package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * An operation that can be performed on a Document.
 *
 * This abstract class provides some base functionality which should be applicable to all Stages.
 *
 * Config Parameters:
 *
 * - conditional_fields (List<String>, Optional) : The fields which will be used to determine if this stage should be applied.
 * Turns off conditional execution by default.
 * - conditional_values (List<String>, Optional) : The values which we will search the conditional fields for.
 * Should be set iff conditional_fields is set.
 * - conditional_operator (String, Optional) : The operator to determine conditional execution.
 * Can be 'must' or 'must_not'. Defaults to must.
 */
public abstract class Stage {

  protected Config config;
  private final List<String> conditionalFields;
  private final List<String> conditionalValues;
  private final String operator;

  // TODO : Debug mode
  public Stage(Config config) {
    this.config = config;

    this.conditionalFields = StageUtils.configGetOrDefault(config, "conditional_field", new ArrayList<>());
    this.conditionalValues = StageUtils.configGetOrDefault(config, "conditional_values", new ArrayList<>());
    this.operator = StageUtils.configGetOrDefault(config, "conditional_operator", "must");
  }

  public void start() throws StageException {
  }

  /**
   * Determines if this Stage should process this Document based on the conditional execution parameters. If no no
   * conditionalFields are supplied in the config, the Stage will always execute. If none of the provided conditionalFields
   * are present on the given document, this should behave the same as if the fields were present but none of the supplied
   * values were found in the fields.
   *
   * @param doc the doc to determine processing for
   * @return  boolean representing - should we process?
   */
  public boolean shouldProcess(Document doc) {
    boolean ifFound = operator.equalsIgnoreCase("must");
    List<String> validFields = conditionalFields.stream().filter(doc::has).collect(Collectors.toList());

    if (conditionalFields.isEmpty()) {
      return true;
    }

    if (validFields.isEmpty()) {
      return !ifFound;
    }

    for (String field : validFields) {
      for (String value : doc.getStringList(field)) {
        if (conditionalValues.contains(value)) {
          return ifFound;
        }
      }
    }

    return !ifFound;
  }

  /**
   * Process this Document iff it adheres to our conditional requirements.
   *
   * @param doc the Document
   * @return  a list of child documents resulting from this Stages processing
   * @throws StageException
   */
  public List<Document> processConditional(Document doc) throws StageException {
    if (shouldProcess(doc)) {
      return processDocument(doc);
    }

    return null;
  }


  /**
   * Applies an operation to a Document in place and returns a list containing any child Documents generated
   * by the operation. If no child Documents are generated, the return value should be null.
   *
   * This interface assumes that the list of child Documents is large enough to hold in memory. To support
   * an unbounded number of child documents, this method would need to return an Iterator (or something similar)
   * instead of a List.
   */
  public abstract List<Document> processDocument(Document doc) throws StageException;

}
