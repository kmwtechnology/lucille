package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
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
 * Extracts the first character from each of the given input fields and set the associated output field
 * to contain the character. If the character is not alphanumeric, then 'nonaplha' will be placed in the destination field.
 *
 * Config Parameters -
 *
 *   - fieldMapping (Map<String, String>) : A mapping of source->destination fields
 *   - replacement (String, Optional) : The String to place in the output field if the first character is not a letter.
 *     If "SKIP" is supplied, the output field will not be set to anything. Defaults to "nonalpha".
 */
public class ExtractFirstCharacter extends Stage {

  private final Map<String, Object> fieldMapping;
  private final String replacement;

  public ExtractFirstCharacter(Config config) {
    super(config);
    this.fieldMapping = config.getConfig("fieldMapping").root().unwrapped();
    this.replacement = config.hasPath("replacement") ? config.getString("replacement") : "nonalpha";
  }

  public void start() throws StageException {
    if (fieldMapping.size() == 0) {
      throw new StageException("field mapping must contain at least one entry");
    }
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    for (Map.Entry<String, Object> entry : fieldMapping.entrySet()) {

      if (!doc.has(entry.getKey()))
        continue;

      String value = doc.getString(entry.getKey());

      if (value == null || value.isBlank())
        continue;

      String firstChar = value.substring(0, 1);
      String dest = (String) entry.getValue();

      if (StringUtils.isAlpha(firstChar)) {
        doc.update(dest, UpdateMode.DEFAULT, firstChar);
      } else {
        if (!replacement.equals("SKIP")) {
          doc.update(dest, UpdateMode.DEFAULT, replacement);
        }
      }
    }

    return null;

  }
}
