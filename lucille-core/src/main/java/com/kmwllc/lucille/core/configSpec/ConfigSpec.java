package com.kmwllc.lucille.core.configSpec;

import com.typesafe.config.Config;
import java.util.Set;

public interface ConfigSpec {

  /**
   * Returns this ConfigSpec with the given properties added as required properties.
   * @param properties The required properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required properties added.
   */
  ConfigSpec withRequiredProperties(String... properties);

  /**
   * Returns this ConfigSpec with the given properties added as optional properties.
   * @param properties The optional properties you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional properties added.
   */
  ConfigSpec withOptionalProperties(String... properties);

  /**
   * Returns this ConfigSpec with the given parents added as required parents.
   * @param properties The required parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given required parents added.
   */
  ConfigSpec withRequiredParents(String... properties);

  /**
   * Returns this ConfigSpec with the given parents added as optional parents.
   * @param properties The optional parents you want to add to this ConfigSpec.
   * @return This ConfigSpec with the given optional parents added.
   */
  ConfigSpec withOptionalParents(String... properties);

  /**
   * Sets this ConfigSpec to have the given display name.
   * @param newDisplayName The new display name for this ConfigSpec.
   */
  void setDisplayName(String newDisplayName);

  /**
   * Validates this Config using this ConfigSpec's properties. Throws an Exception if the Config is missing a required
   * parent / property or contains a non-legal property.
   * @param config The Config that you want to validate against this ConfigSpec's properties.
   */
  void validate(Config config);

  /**
   * Returns a Set of the legal properties associated with this ConfigSpec.
   * @return a Set of the legal properties associated with this ConfigSpec.
   */
  Set<String> getLegalProperties();

}
