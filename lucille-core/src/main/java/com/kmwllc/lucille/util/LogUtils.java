package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;

public class LogUtils {

  public static final Spec SPEC = SpecBuilder.withoutDefaults()
      .optionalNumber("seconds").build();

  public static final int DEFAULT_LOG_SECONDS = 30;

  // name of default metrics registry
  public static final String METRICS_REG = "default";
}
