package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.concurrent.ThreadLocalRandom;

/**
 * A stage that drops documents that pass through it. Intended to be used with the conditional framework.
 * By default, this stage drops 100% of documents.
 *
 * If an optional percentage parameter is provided each document will be dropped independently with that probability. This
 * probabilistic behavior is used primarily for testing to simulate loss in a non-deterministic way.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>percentage (Double, Optional) : the probability in the inclusive range 0.0-1.0 indicating the chance that an incoming
 *   document will be dropped, defaults to 1.0.</li>
 * </ul>
 */
public class DropDocument extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalNumber("percentage")
      .build();

  private final double percentage;

  public DropDocument(Config config) throws StageException {
    super(config);
    Number percent = ConfigUtils.getOrDefault(config, "percentage", 1.0);
    this.percentage = percent.doubleValue(); // Need to convert to double here or passing in 1.0 fails.

    if (percentage < 0.0 || percentage > 1.0) {
      throw new StageException("Percentage must be between 0.0 and 1.0.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (percentage == 1.0 || (percentage > 0.0 && ThreadLocalRandom.current().nextDouble() < percentage)) {
      doc.setDropped(true);
    }

    return null;
  }
}
