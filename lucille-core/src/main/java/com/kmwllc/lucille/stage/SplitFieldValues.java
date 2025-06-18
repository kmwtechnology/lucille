package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import java.util.Iterator;
import java.util.List;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Splits a field based off a delimiter and places the separated values into a given output field.
 * <p>
 * Config Parameters -
 * <ul>
 * <li>inputField (String) : The field to split on.</li>
 * <li>outputField (String) : The field to place the separated values into.</li>
 * <li>delimiter (String) : The String to split the field by.</li>
 * <li>trimWhitespace (Boolean) : True if separated values should trim whitespace, false otherwise.</li>
 * </ul>
 */
public class SplitFieldValues extends Stage {

  public static Spec SPEC = Spec.stage()
      .reqStr("inputField", "outputField", "delimiter")
      .reqBool("trimWhitespace");

  private boolean trimWhitespace = true;
  private String inputField;
  private String outputField;
  private String delimiter;

  public SplitFieldValues(Config config) {
    super(config);

    inputField = config.getString("inputField");
    outputField = config.getString("outputField");
    delimiter = config.getString("delimiter");
    trimWhitespace = config.getBoolean("trimWhitespace");

  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // operates only on the first value in the field at the moment?
    if (doc.has(inputField)) {
      List<String> fieldValues = doc.getStringList(inputField);
      if (inputField.equals(outputField)) {
        doc.removeField(outputField);
      }
      for (String value : fieldValues) {
        String[] values = value.split(delimiter);
        for (String val : values) {
          if (trimWhitespace) {
            val = val.trim();
          }
          doc.addToField(outputField, val);
        }
      }
    }
    return null;
  }

}
