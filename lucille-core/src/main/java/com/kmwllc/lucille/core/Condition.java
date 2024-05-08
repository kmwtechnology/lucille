package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;

public class Condition implements Predicate<Document> {

  private final List<String> fields;
  private final Set<String> values;
  private final Operator operator;

  private enum Operator {
    MUST("must"), MUST_NOT("must_not");

    private final String val;

    Operator(String val) {
      this.val = val;
    }

    public static Operator get(String operator) {
      Optional<Operator> temp = Arrays.stream(Operator.values()).filter(other -> other.val.equals(operator)).findFirst();
      if (temp.isPresent()) {
        return temp.get();
      }
      throw new IllegalArgumentException("operator is invalid. It must take the value of `must` or `must_not`");
    }
  }

  public Condition(Config config) {
    this(config.getStringList("fields"), new HashSet<>(config.getStringList("values")),
        config.hasPath("operator") ? Operator.get(config.getString("operator")) : Operator.MUST);
  }

  public Condition(List<String> fields, Set<String> values, Operator operator) {
    this.fields = fields;
    this.values = values;
    this.operator = operator;
  }

  public static Condition fromConfig(Config config) {
    return new Condition(config);
  }

  @Override
  public boolean test(Document doc) {
    boolean resultWhenValueFound = operator.equals(Operator.MUST);

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
