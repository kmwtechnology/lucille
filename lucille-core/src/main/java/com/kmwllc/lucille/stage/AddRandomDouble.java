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
 * Adds random Dates to documents given parameters.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldName (String, Optional) : Field name of field where data is placed. Defaults to "data".</li>
 *   <li>rangeStart (Double, Optional) : Double representing the start of the range for generating random doubles. Defaults to 0.0.</li>
 *   <li>rangeEnd (Double, Optional) : Double representing the end of the range for generating random doubles. Defaults to 1M (1000000.0).</li>
 * </ul>
 */
public class AddRandomDouble extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("fieldName")
      .optionalNumber("rangeStart", "rangeEnd").build();

  private final String fieldName;
  private final double rangeStart;
  private final double rangeEnd;

  public AddRandomDouble(Config config) throws StageException {
    super(config);

    this.fieldName = ConfigUtils.getOrDefault(config, "fieldName", "data");
    this.rangeStart = ConfigUtils.getOrDefault(config, "rangeStart", 0.0);
    this.rangeEnd = ConfigUtils.getOrDefault(config, "rangeEnd", 1000000.0);

    if (rangeStart > rangeEnd) {
      throw new StageException("Provided rangeStart is greater than the rangeEnd.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(fieldName, ThreadLocalRandom.current().nextDouble(rangeStart, rangeEnd));

    return null;
  }
}
