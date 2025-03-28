package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import java.util.Set;

public interface ConfigSpec {

  ConfigSpec withRequiredProperties(String... properties);

  ConfigSpec withOptionalProperties(String... properties);

  ConfigSpec withRequiredParents(String... properties);

  ConfigSpec withOptionalParents(String... properties);

  void validate(Config config);

  Set<String> getLegalProperties();

}
