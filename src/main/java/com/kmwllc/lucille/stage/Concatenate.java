package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import org.apache.commons.lang.text.StrSubstitutor;

import java.util.HashMap;
import java.util.List;

/**
 * This Stage replaces wildcards in a given format String with the value for the given field. To declare a wildcard,
 * surround the name of the field with '{}'. EX: "{city}, {state}, {country}" -> "Boston, MA, USA". NOTE: If a given
 * field is multivalued, this Stage will substitute the first value for every wildcard.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names
 *   - dest (String) : Destination field. This Stage only supports supplying a single destination field.
 *   - format_string (String) : The format String, which will have field values substituted into its placeholders
 */
public class Concatenate extends Stage {

  private final List<String> sourceFields;
  private final String destField;
  private final String formatStr;

  public Concatenate(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destField = config.getString("dest");
    this.formatStr = config.getString("format_string");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Concatenate");
  }

  // NOTE : If a given field is multivalued, this Stage will only operate on the first value
  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    HashMap<String, String> replacements = new HashMap<>();
    for (String source : sourceFields) {
      if (!doc.has(source))
        continue;

      // For each source field, add the field name and first value to our replacement map
      replacements.put(source, doc.getStringList(source).get(0));
    }

    // TODO : Consider making Document a Map
    // Substitute all of the {field} formatters in the string with the field value.
    StrSubstitutor sub = new StrSubstitutor(replacements, "{", "}");
    doc.setField(destField, sub.replace(formatStr));

    return null;
  }
}
