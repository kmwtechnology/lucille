package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;

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
  private String name;

  // TODO : Debug mode
  public Stage(Config config) {
    this.config = config;
    this.name = ConfigUtils.getOrDefault(config, "name", null);
    this.conditionalFields = ConfigUtils.getOrDefault(config, "conditional_field", new ArrayList<>());
    this.conditionalValues = ConfigUtils.getOrDefault(config, "conditional_values", new ArrayList<>());
    this.operator = ConfigUtils.getOrDefault(config, "conditional_operator", "must");
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
    boolean resultWhenValueFound = operator.equalsIgnoreCase("must");

    if (conditionalFields.isEmpty()) {
      return true;
    }

    for (String field : conditionalFields) {
      if (!doc.has(field)) {
        continue;
      }

      for (String value : doc.getStringList(field)) {
        if (conditionalValues.contains(value)) {
          return resultWhenValueFound;
        }
      }
    }

    return !resultWhenValueFound;
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

  public String getName() {
    return name;
  }

  public void initializeName(String name) throws StageException {
    if (this.name!=null) {
      throw new StageException("Stage name cannot be changed after it has been initialized.");
    }
    this.name = name;
  }

  public Config getConfig() {
    return config;
  }

}
