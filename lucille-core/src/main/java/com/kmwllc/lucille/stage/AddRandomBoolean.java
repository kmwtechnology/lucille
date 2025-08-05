package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Adds random Booleans to documents given parameters.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>field_name (String, Optional) : Field name of field where data is placed. Defaults to "data".</li>
 *   <li>percent_true (Integer, Optional) : Determines the rate at which "true" is selected and placed on a document. Defaults to 50.</li>
 * </ul>
 */
public class AddRandomBoolean extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("field_name")
      .optionalNumber("percent_true").build();

  private final String fieldName;
  private final int percentTrue;

  public AddRandomBoolean(Config config) throws StageException {
    super(config);

    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.percentTrue = ConfigUtils.getOrDefault(config, "percent_true", 50);

    if (percentTrue > 100 || percentTrue < 0) {
      throw new StageException("Invalid value for percent_true. Must be greater than 0 and less than 100.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(fieldName, (ThreadLocalRandom.current().nextInt(100) < percentTrue));

    return null;
  }

}
