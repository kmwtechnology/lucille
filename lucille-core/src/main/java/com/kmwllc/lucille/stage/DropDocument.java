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
 * If an optional percentToDrop parameter is provided, each document is dropped independently with that likelihood.
 * This probabilistic behavior is used primarily for testing to simulate loss in a non-deterministic way.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>percentToDrop (Double, Optional) : the percentage, in the inclusive range 0-100, of incoming documents to drop;
 *   each document is decided independently. Defaults to 100.</li>
 * </ul>
 */
public class DropDocument extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalNumber("percentToDrop")
      .build();

  private final double percentToDrop;

  public DropDocument(Config config) throws StageException {
    super(config);
    Number percent = ConfigUtils.getOrDefault(config, "percentToDrop", 100.0);
    this.percentToDrop = percent.doubleValue(); // Need to convert to double here or passing in 100 fails.

    if (percentToDrop < 0.0 || percentToDrop > 100.0) {
      throw new StageException("percentToDrop must be between 0 and 100.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (percentToDrop == 100.0
        || (percentToDrop > 0.0 && ThreadLocalRandom.current().nextDouble() * 100.0 < percentToDrop)) {
      doc.setDropped(true);
    }

    return null;
  }
}
