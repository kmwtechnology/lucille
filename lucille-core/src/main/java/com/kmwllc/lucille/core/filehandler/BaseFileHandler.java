package com.kmwllc.lucille.core.filehandler;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * The base implementation of a FileHandler. Validates the handler's Config against its Spec and reads
 * <code>docIdPrefix</code>. Publishing comes from {@link FileHandler#processFileAndPublish}.
 */
public abstract class BaseFileHandler implements FileHandler {

  protected final String docIdPrefix;

  public BaseFileHandler(Config config) {
    Spec.of(getClass()).validate(config, getClass().getName());

    this.docIdPrefix = ConfigUtils.getStringOrDefault(config, "docIdPrefix", "");
  }
}
