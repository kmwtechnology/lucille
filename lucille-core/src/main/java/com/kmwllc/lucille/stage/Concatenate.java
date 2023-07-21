package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.apache.commons.lang.text.StrSubstitutor;

import java.util.*;

/**
 * Replaces wildcards in a given format String with the value for the given field. To declare a
 * wildcard, surround the name of the field with '{}'. EX: "{city}, {state}, {country}" -> "Boston,
 * MA, USA". NOTE: If a given field is multivalued, this Stage will substitute the first value for
 * every wildcard.
 *
 * <p>Config Parameters:
 *
 * <p>- source (List<String>) : list of source field names - dest (String) : Destination field. This
 * Stage only supports supplying a single destination field. - format_string (String) : The format
 * String, which will have field values substituted into its placeholders - defualt_inputs
 * (Map<String, String>, Optional) : Mapping of input fields to a default value. You do not have to
 * supply a default for every input field, if a default is not provided, the default behavior will
 * be to leave the wildcard for the field in place. Defaults to an empty Map. - update_mode (String,
 * Optional) : Determines how writing will be handling if the destination field is already
 * populated. Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class Concatenate extends Stage {

  private final String destField;
  private final String formatStr;
  private final Map<String, Object> defaultInputs;
  private final UpdateMode updateMode;

  private final List<String> fields;

  public Concatenate(Config config) {
    super(
        config,
        new StageSpec()
            .withRequiredProperties("dest", "format_string")
            .withOptionalProperties("update_mode")
            .withOptionalParents("default_inputs"));

    this.destField = config.getString("dest");
    this.formatStr = config.getString("format_string");
    this.defaultInputs =
        config.hasPath("default_inputs")
            ? config.getConfig("default_inputs").root().unwrapped()
            : new HashMap<>();
    // defaultInputs = set.stream().collect(Collectors.toMap(Entry::getKey, Entry::getValue));
    this.updateMode = UpdateMode.fromConfig(config);
    this.fields = new ArrayList<>();
  }

  @Override
  public void start() throws StageException {
    Scanner scan = new Scanner(formatStr);
    for (String s; (s = scan.findWithinHorizon("(?<=\\{).*?(?=})", 0)) != null; ) {
      fields.add(s);
    }
  }

  // NOTE : If a given field is multivalued, this Stage will only operate on the first value
  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    HashMap<String, String> replacements = new HashMap<>();
    for (String source : fields) {
      String value;
      if (!doc.has(source)) {
        if (defaultInputs.containsKey(source)) {
          value = (String) defaultInputs.get(source);
        } else {
          continue;
        }
      } else {
        value = doc.getStringList(source).get(0);
      }

      // For each source field, add the field name and first value to our replacement map
      replacements.put(source, value);
    }

    // TODO : Consider making Document a Map
    // Substitute all of the {field} formatters in the string with the field value.
    StrSubstitutor sub = new StrSubstitutor(replacements, "{", "}");
    doc.update(destField, updateMode, sub.replace(formatStr));

    return null;
  }
}
