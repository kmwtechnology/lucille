package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * This stage will use the java object hash code modulus the number of buckets as specified in the buckets parameter, 
 * the resulting label will be placed in the dest field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>fieldName (String, Required) : Field that will be used as the input for the hashing function.</li>
 *   <li>dest (String, Required) : Field that will contain the hash bucket label.</li>
 *   <li>buckets (List of String, Required) : list of buckets for the hash function.</li>
 * </ul>
 */
public class HashFieldValueToBucket extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("fieldName", "dest")
      .requiredList("buckets", new TypeReference<List<String>>(){}).build();

  private final String fieldName;
  private final List<String> buckets;
  private final String destField;
  private final int numBuckets;

  public HashFieldValueToBucket(Config config) throws StageException {
    super(config);
    this.fieldName = config.getString("fieldName");
    this.buckets = config.getStringList("buckets");
    this.destField = config.getString("dest");
    this.numBuckets = buckets.size();
    if (buckets.size() == 0) {
      throw new StageException("There must be at least one bucket defined in the buckets parameter.");
    }
    if (StringUtils.isEmpty(fieldName)) {
      throw new StageException("fieldName must not be null or empty");
    }
    if (StringUtils.isEmpty(destField)) {
      throw new StageException("dest field name must not be null or empty");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    int hashIndex = Math.abs(doc.getId().hashCode() % numBuckets);
    doc.setField(destField, buckets.get(hashIndex));
    return null;
  }

}