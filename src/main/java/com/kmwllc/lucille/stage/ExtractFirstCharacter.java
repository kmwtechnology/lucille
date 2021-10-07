package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigValue;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This Stage will extract the first character from each of the given input fields and set the associated output field
 * to contain the character. If the character is not alphanumeric, then 'nonaplha' will be placed in the destination field.
 *
 * Config Parameters -
 *
 *   fieldMapping (Map<String, String>) : A mapping of source->destination fields
 */
public class ExtractFirstCharacter extends Stage {

  private final Set<Map.Entry<String, ConfigValue>> fieldMapping;

  public ExtractFirstCharacter(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").entrySet();
  }

  public void start() throws StageException {
    if (fieldMapping.size() == 0) {
      throw new StageException("field mapping must contain at least one entry");
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (Map.Entry<String, ConfigValue> entry : fieldMapping) {

      if (!doc.has(entry.getKey()))
        continue;

      String firstChar = doc.getString(entry.getKey()).substring(0, 1);
      String dest = (String) entry.getValue().unwrapped();

      if (StringUtils.isAlpha(firstChar)) {
        doc.update(dest, UpdateMode.DEFAULT, firstChar);
      } else {
        doc.update(dest, UpdateMode.DEFAULT, "nonalpha");
      }
    }

    return null;

  }
}
