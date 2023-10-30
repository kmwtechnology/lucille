package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sets the value of given fields to a mapped value.
 * <p>
 * Config Parameters -
 * <ul>
 * <li>static_values (Map<String, Object>) : A mapping from the field to the value.</li>
 * <li>updateMode (UpdateMode) : The update mode to use when updating the fields.</li>
 * </ul>
 */
public class SetStaticValues extends Stage {

  private final Map<String, Object> staticValues;
  private final UpdateMode updateMode;

  public SetStaticValues(Config config) {
    super(config, new StageSpec()
        .withOptionalProperties("update_mode")
        .withRequiredParents("static_values"));
    staticValues = config.getConfig("static_values").root().unwrapped();
    updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    staticValues.forEach((fieldName, staticValue) -> doc.update(fieldName, updateMode, (String) staticValue));
    return null;
  }
}
