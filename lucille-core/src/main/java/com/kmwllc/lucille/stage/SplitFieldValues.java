package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.util.StageUtils;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

/**
 * Splits a field based off a delimiter and places the separated values into a given output field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>inputField (String) : The field to split on.</li>
 *   <li>outputField (String) : The field to place the separated values into.</li>
 *   <li>delimiter (String) : Java regular expression to split the field by, so characters such as '|' or '.' must be escaped.
 *   Trailing empty pieces are dropped. An invalid expression fails the stage at startup.</li>
 *   <li>trimWhitespace (Boolean) : True if separated values should trim whitespace, false otherwise.</li>
 * </ul>
 */
public class SplitFieldValues extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("inputField", "outputField", "delimiter")
      .requiredBoolean("trimWhitespace").build();

  private boolean trimWhitespace = true;
  private final String inputField;
  private final String outputField;
  private final String delimiter;
  private Pattern delimiterPattern;

  public SplitFieldValues(Config config) {
    super(config);

    inputField = config.getString("inputField");
    outputField = config.getString("outputField");
    delimiter = config.getString("delimiter");
    trimWhitespace = config.getBoolean("trimWhitespace");

  }

  @Override
  public void start() throws StageException {
    try {
      // compiled once, so an invalid regex fails at startup instead of on every document
      delimiterPattern = Pattern.compile(delimiter);
    } catch (PatternSyntaxException e) {
      throw new StageException("Invalid delimiter regex: " + delimiter, e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // operates only on the first value in the field at the moment?
    List<String> fieldValues = StageUtils.getNonNullStrings(doc, inputField);
    if (!fieldValues.isEmpty()) {
      if (inputField.equals(outputField)) {
        doc.removeField(outputField);
      }
      for (String value : fieldValues) {
        String[] values = delimiterPattern.split(value);
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
