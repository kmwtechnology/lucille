package com.kmwllc.lucille.stage;

import java.text.Normalizer;
import java.util.Iterator;
import java.util.List;
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
public class CleanText extends Stage {

  private final String source;
  private final List<String> whitelist;
  private final List<String> blacklist;


  public CleanText(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source"));
    this.source = config.getString("source");
    this.whitelist = ConfigUtils.getOrDefault(config, "whitelist", null);
    this.blacklist = ConfigUtils.getOrDefault(config, "blacklist", null);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (!doc.has(source)) {
      return null;
    }

    doc.setField(source, Normalizer.normalize(doc.getString(source), Normalizer.Form.NFKD));
    return null;
  }

}
