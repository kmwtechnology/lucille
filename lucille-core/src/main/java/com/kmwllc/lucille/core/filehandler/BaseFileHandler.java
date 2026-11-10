package com.kmwllc.lucille.core.filehandler;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.spec.Spec;
import com.typesafe.config.Config;

/**
 * The base implementation of a FileHandler. Validates the handler's Config against its Spec and applies
 * <code>docIdPrefix</code> through {@link #createDocId(String)}. Publishing comes from
 * {@link FileHandler#processFileAndPublish}.
 */
public abstract class BaseFileHandler implements FileHandler {

  private final String docIdPrefix;

  public BaseFileHandler(Config config) {
    Spec.of(getClass()).validate(config, getClass().getName());

    this.docIdPrefix = ConfigUtils.getStringOrDefault(config, "docIdPrefix", "");
  }

  /**
   * Creates the id of a Document this handler extracts, by applying the configured <code>docIdPrefix</code> to the given
   * id. Every Document a handler creates should get its id from here, so that the prefix is applied the same way by
   * every handler.
   *
   * <p>A handler reading a record whose id may be blank should check it with {@link Document#isValidId(String)} first,
   * and skip the record if it is not valid. This method refuses a null or blank id, instead of building an id that would
   * be shared by every such record.
   *
   * @param id The id built from the record, before the prefix is applied.
   * @return The Document id: the configured <code>docIdPrefix</code> followed by the given id.
   * @throws IllegalArgumentException if the id is null or blank
   */
  protected final String createDocId(String id) {
    if (!Document.isValidId(id)) {
      throw new IllegalArgumentException("Cannot create a document id from a null or blank id.");
    }
    return docIdPrefix + id;
  }
}
