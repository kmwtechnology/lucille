package com.kmwllc.lucille.stage;

import java.util.Iterator;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;

/**
 * Truncates a string field to a given number of characters. Can be inplace or return the result to a 
 * seperate field
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>source</b> (String) : The field to be truncated. If this is not a string it is still parsed as a string to generate a result
 * </p>
 * <p>
 * <b>max_size</b> (int) : The maximum number of characters to truncate the input to. If this is negative a StageException 
 * is thrown. 
 *  </p>
 * <p>
 * <p>
 * <b>destination</b> (String, Optional) : The field where the truncated data should be placed. If this is not provided the 
 * operation is done inplace. If this field already exists it is overwritten.
 * </p>
 */
public class TruncateField extends Stage {

  private final String source;
  private final int maxSize;
  private final String destination;

  public TruncateField(Config config) {
    super(config, new StageSpec().withRequiredProperties("source", "max_size").withOptionalProperties("destination"));


    this.source = config.getString("source");
    this.maxSize = config.getInt("max_size");
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
  }

  @Override
  public void start() throws StageException {
    if(maxSize < 0) {
      throw new StageException("max_size cannot be negative");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (doc.has(source)) {
      String string = doc.getString(source);
      if (string == null) {
        throw new StageException(String.format("Field: {} does not contain a string", source));
      }

      String newString = string.length() <= maxSize ? string : string.substring(0, maxSize);
      doc.update(destination != null ? destination : source, UpdateMode.OVERWRITE, newString);
    }
    return null;
  }
}
