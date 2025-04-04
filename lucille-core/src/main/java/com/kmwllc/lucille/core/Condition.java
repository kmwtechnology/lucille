package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigValue;
import com.typesafe.config.ConfigValueType;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
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
    this(config.getStringList("fields"), createValueSet(config),
        config.hasPath("operator") ? Operator.get(config.getString("operator")) : Operator.MUST);
  }

  private static Set<String> createValueSet(Config config) {
    if (!config.hasPath("values")) {
      return null;
    }

    ConfigList list = config.getList("values");
    HashSet<String> mySet = new HashSet<>();
    for (ConfigValue val : list) {
      if (val.valueType() == ConfigValueType.NULL) {
        mySet.add(null);
      } else {
        // Right now condition matching is non-restrictive, ie.
        //    a field with a boolean value, ex. true, will match both values "true" and true.
        //    a field with an int value, ex. 10, will match both values "10" and 10.
        // This is because all value types other than null is unwrapped and stringified.
        mySet.add(val.unwrapped().toString());
      }
    }
    return mySet;
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

    if (values != null) {
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
    } else {
      if (resultWhenValueFound) {
        return fields.stream().allMatch(field -> doc.has(field));
      } else {
        return fields.stream().allMatch(field -> !doc.has(field));
      }
    }


    return !resultWhenValueFound;
  }
}
