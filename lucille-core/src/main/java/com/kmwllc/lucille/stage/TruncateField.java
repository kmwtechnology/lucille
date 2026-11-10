package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import java.util.List;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

/**
 * Truncates every value of a string field to a given number of characters. Can be inplace or return the result to a 
 * seperate field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (String) : The field to be truncated. If this is not a string it is still parsed as a string to generate a result.
 *   If a document does not have the field, or it holds only nulls, it will be skipped.</li>
 *   <li>maxLength (int) : The maximum number of characters to truncate the input to. If this is negative a StageException is thrown. </li>
 *   <li>dest (String, Optional) : The field where the truncated data should be placed. If this is not provided the operation
 *   fis done inplace. If this field already exists it is overwritten.</li>
 * </ul>
 */
public class TruncateField extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("source")
      .requiredNumber("maxLength")
      .optionalString("dest").build();

  private final String source;
  private final int maxSize;
  private final String destination;

  public TruncateField(Config config) {
    super(config);


    this.source = config.getString("source");
    this.maxSize = config.getInt("maxLength");
    this.destination = ConfigUtils.getStringOrDefault(config, "dest", null);
  }

  @Override
  public void start() throws StageException {
    if (maxSize < 0) {
      throw new StageException("maxLength cannot be negative");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    List<String> values = StageUtils.getNonNullStrings(doc, source);
    if (values.isEmpty()) {
      return null;
    }

    String[] truncated = values.stream()
        .map(value -> value.length() <= maxSize ? value : value.substring(0, maxSize))
        .toArray(String[]::new);
    doc.update(destination != null ? destination : source, UpdateMode.OVERWRITE, truncated);

    return null;
  }
}
