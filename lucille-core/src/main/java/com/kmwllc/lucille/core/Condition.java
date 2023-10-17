package com.kmwllc.lucille.core;

import com.typesafe.config.Config;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;

public class Condition implements Predicate<Document> {

  private final List<String> fields;
  private final Set<String> values;
  private final String operator;

  public Condition(Config config) {
    this(config.getStringList("fields"),
        new HashSet<>(config.getStringList("values")),
        config.hasPath("operator") ? config.getString("operator") : "must");
  }

  public Condition(List<String> fields, Set<String> values, String operator) {
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
