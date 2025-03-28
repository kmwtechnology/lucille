package com.kmwllc.lucille.core;

import com.typesafe.config.Config;
import java.util.List;
import java.util.Set;

/**
 * A ConfigSpec for a Stage.
 */
public class StageSpec extends BaseConfigSpec {

  private static final Set<String> DEFAULT_STAGE_PROPERTIES = Set.of("name", "class", "conditions", "conditionPolicy");

  /**
   * Creates a StageSpec.
   */
  public StageSpec() {
    super();
  }

  @Override
  protected Set<String> getDefaultSpecProperties() {
    return DEFAULT_STAGE_PROPERTIES;
  }

  /**
   * Validates the given config against the given properties as a StageSpec, thereby using the StageSpec's default properties
   * (name, class, etc.).
   *
   * @param config The configuration you want to validate.
   * @param requiredProperties The properties you require in your config.
   * @param optionalProperties The properties allowed in your config.
   * @param requiredParents The parents you require in your config.
   * @param optionalParents The parents you allow in your config.
   */
  public static void validateConfig(Config config, List<String> requiredProperties, List<String> optionalProperties, List<String> requiredParents, List<String> optionalParents) {
    ConfigSpec spec = new StageSpec()
        .withRequiredProperties(requiredProperties.toArray(new String[0]))
        .withOptionalProperties(optionalProperties.toArray(new String[0]))
        .withRequiredParents(requiredParents.toArray(new String[0]))
        .withOptionalParents(optionalParents.toArray(new String[0]));

    spec.validate(config);
  }
}
