package com.kmwllc.lucille.stage;

import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

public class SplitFieldValues extends Stage {

  private boolean trimWhitespace = true;
  private String inputField;
  private String outputField;
  private String delimiter;
  
  public SplitFieldValues(Config config) {
    super(config);
    // 
    inputField = config.getString("inputField");
    outputField = config.getString("outputField");
    delimiter = config.getString("delimiter");
    trimWhitespace = config.getBoolean("trimWhitespace");
    
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // operates only on the first value in the field at the moment?
    if (doc.has(inputField)) {
      String[] values = doc.getString(inputField).split(delimiter);
      if (inputField.equals(outputField)) {
        doc.removeField(outputField);
      }
      for (String val : values) {
        if (trimWhitespace) {
          val = val.trim();
        }
        doc.addToField(outputField, val);
      }
    }
    return null;
  }

}
