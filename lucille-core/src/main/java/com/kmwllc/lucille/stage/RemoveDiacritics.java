package com.kmwllc.lucille.stage;

import java.text.Normalizer;
import java.util.Iterator;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;


/**
 * Adds random data to a document field given parameters
 * <br>
 * Config Parameters -
 * <br>
 * <p>
 * <b>inputDataPath</b> (String, Optional) : file path to a text file that stores datapoints to be randomly placed into field,
 *  defaults to numeric data based on range size (0 -&gt; rangeSize - 1)
 * </p>
 * <p>
 * <b>fieldName</b> (String, Optional) : Field name of field where data is placed, defaults to "data"
 *  </p>
 * <p>
 * <b>rangeSize</b> (int, Optional) : size of the subset of datapoints to be grabbed either from
 *  given datapath or from random numbers
 * </p>
 *  <p>
 * <b>minNumOfTerms</b> (Integer, Optional) : minimum number of terms to be in the field, defaults to 1
 * </p>
 * <p>
 * <b>maxNumOfTerms</b> (Integer, Optional) : maximum number of terms to be in the field, defaults to 1
 * </p>
 * <p>
 * <b>isNested</b> (FieldType, Optional) : setting for structure of field, default or nested
 * </p>
 */
public class RemoveDiacritics extends Stage {

  private final String source;
  private final String destination;


  public RemoveDiacritics(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source").withOptionalProperties("destination"));
    this.source = config.getString("source");
    this.destination = ConfigUtils.getOrDefault(config, "destination", null);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    String output = Normalizer.normalize(doc.getString(source), Normalizer.Form.NFKD).replaceAll("\\p{M}", "");

    if (destination == null) {
       doc.setField(source, output);
    } else {
       doc.setField(destination, output);
    }
    return null;
  }
}
