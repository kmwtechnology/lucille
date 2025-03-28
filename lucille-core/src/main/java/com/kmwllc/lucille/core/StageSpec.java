package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Set;

public class StageSpec extends BaseConfigSpec {

  private static final Set<String> DEFAULT_STAGE_PROPERTIES = Set.of("name", "class", "conditions", "conditionPolicy");

  // TODO: Require the stage name to be supplied.
  public StageSpec() {
    super();
  }

  public static void validateConfig(Config config, List<String> requiredProperties, List<String> optionalProperties, List<String> requiredParents, List<String> optionalParents) {
    ConfigSpec spec = new StageSpec()
        .withRequiredProperties(requiredProperties.toArray(new String[0]))
        .withOptionalProperties(optionalProperties.toArray(new String[0]))
        .withRequiredParents(requiredParents.toArray(new String[0]))
        .withOptionalParents(optionalParents.toArray(new String[0]));

    spec.validate(config);
  }

  @Override
  protected Set<String> getDefaultSpecProperties() {
    return DEFAULT_STAGE_PROPERTIES;
  }
}
