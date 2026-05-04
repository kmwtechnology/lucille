package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.Map;

/**
 * Sets the value of given fields to a mapped value.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>staticValues (Map&lt;String, Object&gt;) : A mapping from the field to the value.</li>
 *   <li>updateMode (UpdateMode) : The update mode to use when updating the fields.</li>
 * </ul>
 */
public class SetStaticValues extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("updateMode")
      .optionalBoolean("skip")
      .requiredParent("staticValues", new TypeReference<Map<String, Object>>() {}).build();

  private final Map<String, Object> staticValues;
  private final UpdateMode updateMode;
  private final Boolean skip;

  public SetStaticValues(Config config) {
    super(config);

    this.staticValues = config.getConfig("staticValues").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
    this.skip = config.hasPath("skip") ? config.getBoolean("skip") : false;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    staticValues.forEach((fieldName, staticValue) -> doc.update(fieldName, updateMode, staticValue));
    doc.setSkipped(skip);
    return null;
  }
}
