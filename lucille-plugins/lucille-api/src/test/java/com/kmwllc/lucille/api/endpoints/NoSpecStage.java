package com.kmwllc.lucille.api.endpoints;

import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;

/**
 * A Stage subclass with no SPEC, found by ConfigInfo's classpath scan. ConfigInfoTest checks it is left out of the
 * stage list rather than failing it.
 */
public abstract class NoSpecStage extends Stage {

  protected NoSpecStage(Config config) {
    super(config);
  }

}
