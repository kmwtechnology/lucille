package com.kmwllc.lucille.stage;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.solr.common.StringUtils;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Removes empty fields from a document.
 */
public class RemoveEmptyFields extends Stage {

  public RemoveEmptyFields(Config config) {
    super(config);
    // iterate fields that have a blank value or a null value and remove them. 
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // get the field names
    Map<String,Object> data = doc.asMap();
    Set<String> fieldNames = data.keySet();
    for (String fieldName : fieldNames) {
      if (StringUtils.isEmpty(doc.getString(fieldName))) {
        doc.removeField(fieldName);
      }
    }
    return null;
  }

}
