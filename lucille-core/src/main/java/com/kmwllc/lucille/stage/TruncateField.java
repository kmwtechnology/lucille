package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Truncates a string field to a given number of characters. Can be inplace or return the result to a 
 * seperate field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String) : The field to be truncated. If this is not a string it is still parsed as a string to generate a result.
 *   If a document does not have the field it will be skipped. If the fields contents cannot be interpreted as a String the document is
 *   also skipped and this is logged as a warning.</li>
 *   <li>max_size (int) : The maximum number of characters to truncate the input to. If this is negative a StageException is thrown. </li>
 *   <li>destination (String, Optional) : The field where the truncated data should be placed. If this is not provided the operation
 *   fis done inplace. If this field already exists it is overwritten.</li>
 * </ul>
 */
public class TruncateField extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source")
      .requiredNumber("max_size")
      .optionalString("destination").build();

  private final String source;
  private final int maxSize;
  private final String destination;

  private static final Logger log = LoggerFactory.getLogger(TruncateField.class);

  public TruncateField(Config config) {
    super(config);


    this.source = config.getString("source");
    this.maxSize = config.getInt("max_size");
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
  }

  @Override
  public void start() throws StageException {
    if (maxSize < 0) {
      throw new StageException("max_size cannot be negative");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    String string = doc.getString(source);
    if (string == null) {
      log.warn("Field {} does not contain a String. Document is being skipped.", source);
    }

    String newString = string.length() <= maxSize ? string : string.substring(0, maxSize);
    doc.update(destination != null ? destination : source, UpdateMode.OVERWRITE, newString);

    return null;
  }
}
