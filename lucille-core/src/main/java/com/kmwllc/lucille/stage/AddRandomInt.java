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
 *   <li>rangeStart (Integer, Optional) : Integer representing the start of the range for generating random ints. Defaults to 0.</li>
 *   <li>rangeEnd (Integer, Optional) : Integer representing the end of the range for generating random ints. Defaults to 1M (1000000).</li>
 * </ul>
 */
public class AddRandomInt extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("fieldName")
      .optionalNumber("rangeStart", "rangeEnd").build();

  private final String fieldName;
  private final int rangeStart;
  private final int rangeEnd;

  public AddRandomInt(Config config) throws StageException {
    super(config);

    this.fieldName = ConfigUtils.getOrDefault(config, "fieldName", "data");
    this.rangeStart = ConfigUtils.getOrDefault(config, "rangeStart", 0);
    this.rangeEnd = ConfigUtils.getOrDefault(config, "rangeEnd", 1000000);

    if (rangeStart > rangeEnd) {
      throw new StageException("Provided rangeStart is greater than the rangeEnd.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(fieldName, ThreadLocalRandom.current().nextInt(rangeStart, rangeEnd));

    return null;
  }
}
