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
 *   <li>field_name (String, Optional) : Field name of field where data is placed. Defaults to "data".</li>
 *   <li>range_start (Integer, Optional) : Integer representing the start of the range for generating random ints. Defaults to 0.</li>
 *   <li>range_end (Integer, Optional) : Integer representing the end of the range for generating random ints. Defaults to 1M (1000000).</li>
 * </ul>
 */
public class AddRandomInt extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("field_name")
      .optionalNumber("range_start", "range_end").build();

  private final String fieldName;
  private final int rangeStart;
  private final int rangeEnd;

  public AddRandomInt(Config config) throws StageException {
    super(config);

    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.rangeStart = ConfigUtils.getOrDefault(config, "range_start", 0);
    this.rangeEnd = ConfigUtils.getOrDefault(config, "range_end", 1000000);

    if (rangeStart > rangeEnd) {
      throw new StageException("Provided range_start is greater than the range_end.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(fieldName, ThreadLocalRandom.current().nextInt(rangeStart, rangeEnd));

    return null;
  }
}
