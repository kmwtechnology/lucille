package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Adds random Dates to documents given parameters.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>field_name (String, Optional) : Field name of field where data is placed. Defaults to "data".</li>
 *   <li>range_start (Double, Optional) : Double representing the start of the range for generating random doubles. Defaults to 0.0.</li>
 *   <li>range_end (Double, Optional) : Double representing the end of the range for generating random doubles. Defaults to 1M (1000000.0).</li>
 * </ul>
 */
public class AddRandomDouble extends Stage {

  private final String fieldName;
  private final double rangeStart;
  private final double rangeEnd;

  public AddRandomDouble(Config config) throws StageException {
    super(config, Spec.stage()
        .withOptionalProperties("field_name", "range_start", "range_end"));

    this.fieldName = ConfigUtils.getOrDefault(config, "field_name", "data");
    this.rangeStart = ConfigUtils.getOrDefault(config, "range_start", 0.0);
    this.rangeEnd = ConfigUtils.getOrDefault(config, "range_end", 1000000.0);

    if (rangeStart > rangeEnd) {
      throw new StageException("Provided range_start is greater than the range_end.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    doc.setField(fieldName, ThreadLocalRandom.current().nextDouble(rangeStart, rangeEnd));

    return null;
  }
}
