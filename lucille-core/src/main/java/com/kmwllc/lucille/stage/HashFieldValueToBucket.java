package com.kmwllc.lucille.stage;

import java.util.Iterator;
import java.util.List;

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

  private final String fieldName;
	private final List<String> buckets;
	private final String destField;
	private final int numBuckets;

	public HashFieldValueToBucket(Config config) {
		super(config, new StageSpec().withRequiredProperties("field_name", "dest", "buckets"));
		this.fieldName = config.getString("field_name");
		this.buckets = config.getStringList("buckets");
		this.destField = config.getString("dest");
		numBuckets = buckets.size();
	}

	@Override
	public Iterator<Document> processDocument(Document doc) throws StageException {
		int hashIndex = doc.getId().hashCode() % numBuckets;
		if (hashIndex < 0) {
			hashIndex = hashIndex * -1;
		}
		doc.setField(destField, buckets.get(hashIndex));
		return null;
	}

}