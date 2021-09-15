package com.kmwllc.lucille.core;

import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * An operation that can be performed on a Document.
 */
public abstract class Stage {

  protected Config config;
  private final String conditionalField;
  private final List<String> conditionalValues;
  private final String operator;

  // TODO : Debug mode
  public Stage(Config config) {
    this.config = config;

    this.conditionalField = StageUtils.configGetOrDefault(config, "conditional_field", null);
    this.conditionalValues = StageUtils.configGetOrDefault(config, "conditional_values", new ArrayList<>());
    this.operator = StageUtils.configGetOrDefault(config, "conditional_operator", "must");
  }

  public void start() throws StageException {
  }

  public boolean shouldProcess(Document doc) {
    if (conditionalField == null) {
      return true;
    }

    boolean defaultOutput = !operator.equalsIgnoreCase("must_not");

    for (String value : doc.getStringList(conditionalField)) {
      if (conditionalValues.contains(value)) {
        return defaultOutput;
      }
    }

    return !defaultOutput;
  }

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
