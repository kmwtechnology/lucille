package com.kmwllc.lucille.core;

import java.util.List;
import java.util.function.Predicate;

import com.typesafe.config.Config;

public class Condition implements Predicate<Document> {

  private final List<String> fields;
  private final List<String> values;
  private final String operator;

  public Condition(Config config) {
    this(config.getStringList("fields"),
        config.getStringList("values"),
        config.hasPath("operator") ? config.getString("operator") : "must");
  }

  public Condition(List<String> fields, List<String> values, String operator) {
    this.fields = fields;
    this.values = values;
    this.operator = operator;
  }

  public static Condition fromConfig(Config config) {
    return new Condition(config);
  }

  @Override
  public boolean test(Document doc) {
    boolean resultWhenValueFound = operator.equalsIgnoreCase("must");

    if (fields.isEmpty()) {
      return true;
    }

    for (String field : fields) {
      if (!doc.has(field)) {
        continue;
      }

      for (String value : doc.getStringList(field)) {
        if (values.contains(value)) {
          return resultWhenValueFound;
        }
      }
    }

    return !resultWhenValueFound;
  }
}
