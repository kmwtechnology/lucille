package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
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
 * 
 * <br/>
 * Config Parameters -
 * <br/>
 * <p>
 * <b>field_name</b> (String, Required) : Field that will be used as the input for the hashing function.
 * </p>
 * <p>
 * <b>dest</b> (String, Required) : Field that will contain the hash bucket label.
 * </p>
 * <p>
 * <b>buckets</b> (List of String, Required) : list of buckets for the hash function.
 * </p>
 * 
 */
public class HashFieldValueToBucket extends Stage {

  public static final Spec SPEC = Spec.stage()
      .reqStr("field_name", "dest")
      .reqList("buckets", new TypeReference<List<String>>(){});

  private final String fieldName;
  private final List<String> buckets;
  private final String destField;
  private final int numBuckets;

  public HashFieldValueToBucket(Config config) throws StageException {
    super(config);
    this.fieldName = config.getString("field_name");
    this.buckets = config.getStringList("buckets");
    this.destField = config.getString("dest");
    this.numBuckets = buckets.size();
    if (buckets.size() == 0) {
      throw new StageException("There must be at least one bucket defined in the buckets parameter.");
    }
    if (StringUtils.isEmpty(fieldName)) {
      throw new StageException("field_name must not be null or empty");
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