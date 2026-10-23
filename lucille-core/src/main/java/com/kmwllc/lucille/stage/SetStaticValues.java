package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Sets the value of given fields to a mapped value.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>staticValues (Map&lt;String, Object&gt;) : A mapping from the field to the value. A list value produces a multi-valued field.</li>
 *   <li>updateMode (UpdateMode) : The update mode to use when updating the fields.</li>
 *   <li>skipDocument (boolean) : Sets the document to be skipped by the rest of the pipeline. Can be used in place of the {@link SkipDocument}
 *   stage when we want to both skip and delete documents, and don't want to add another stage with duplicate conditions.</li>
 * </ul>
 */
public class SetStaticValues extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("updateMode")
      .optionalBoolean("skipDocument")
      .requiredParent("staticValues", new TypeReference<Map<String, Object>>() {}).build();

  private final Map<String, Object> staticValues;
  private final UpdateMode updateMode;
  private final boolean skipDocument;

  public SetStaticValues(Config config) {
    super(config);

    this.staticValues = config.getConfig("staticValues").root().unwrapped();
    this.updateMode = UpdateMode.fromConfig(config);
    this.skipDocument = ConfigUtils.getOrDefault(config, "skipDocument", false);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    staticValues.forEach((fieldName, staticValue) -> {
      // a list in the config becomes a multi-valued field; update() takes the values as varargs
      Object[] values = staticValue instanceof List ? ((List<?>) staticValue).toArray() : new Object[] {staticValue};
      doc.update(fieldName, updateMode, values);
    });
    if (skipDocument) {
      doc.setSkipped(true);
    }
    return null;
  }
}
