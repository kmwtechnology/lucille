package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Extracts text based on a given regular expression. You can supply a comma separated list of fields to apply the text extraction to
 * multiple fields. Extracted values are added to the field on top of the existing field value.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>source (List&lt;String&gt;) : List of source field names.</li>
 *   <li>dest (List&lt;String&gt;) : List of destination field names. You can either supply the same number of source and destination
 *   fields. For a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.</li>
 *   <li>regex (String) : A regex expression to find matches for. Matches will be extracted and placed in the destination fields. If the
 *   regex includes capturing groups, the value of the first group will be used.</li>
 *   <li>update_mode (String. Optional) : Determines how writing will be handling if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 *   <li>ignore_case (Boolean, Optional) : Determines whether the regex matcher should ignore case. Defaults to false.</li>
 *   <li>multiline (Boolean, Optional) : Determines whether the regex matcher should allow matches across multiple lines. Defaults to false.</li>
 *   <li>dotall (Boolean, Optional) : Turns on the DOTALL functionality for the regex matcher. Defaults to false.</li>
 *   <li>literal (Boolean, Optional) : Toggles treating the regex expression as a literal String. Defaults to false.</li>
 * </ul>
 */
public class ApplyRegex extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredList("source", new TypeReference<List<String>>(){})
      .requiredList("dest", new TypeReference<List<String>>(){})
      .requiredString("regex")
      .optionalString("update_mode")
      .optionalBoolean("ignore_case", "multiline", "dotall", "literal").build();

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String regexExpr;
  private final UpdateMode updateMode;

  private final boolean ignoreCase;
  private final boolean multiline;
  private final boolean dotall;
  private final boolean literal;

  private Pattern pattern;

  public ApplyRegex(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.regexExpr = config.getString("regex");
    this.updateMode = UpdateMode.fromConfig(config);

    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.multiline = ConfigUtils.getOrDefault(config, "multiline", false);
    this.dotall = ConfigUtils.getOrDefault(config, "dotall", false);
    this.literal = ConfigUtils.getOrDefault(config, "literal", false);
  }

  @Override
  public void start() throws StageException {
    List<Integer> flags = new ArrayList<>();

    // Determine which flags the user turned on and set them when generating the pattern.
    if (ignoreCase) {
      flags.add(Pattern.CASE_INSENSITIVE);
    }

    if (multiline) {
      flags.add(Pattern.MULTILINE);
    }

    if (dotall) {
      flags.add(Pattern.DOTALL);
    }

    if (literal) {
      flags.add(Pattern.LITERAL);
    }

    // Add the selected flags to the Pattern
    switch (flags.size()) {
      case (1):
        pattern = Pattern.compile(regexExpr, flags.get(0));
        break;
      case (2):
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1));
        break;
      case (3):
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2));
        break;
      case (4):
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2) | flags.get(3));
        break;
      default:
        pattern = Pattern.compile(regexExpr);
        break;
    }

    StageUtils.validateFieldNumNotZero(sourceFields, "Apply Regex");
    StageUtils.validateFieldNumNotZero(destFields, "Apply Regex");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Apply Regex");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField)) {
        continue;
      }

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        Matcher matcher = pattern.matcher(value);
        int group = matcher.groupCount() > 0 ? 1 : 0;

        // If we find regex matches in the text, add them to the output field
        while (matcher.find()) {
          outputValues.add(matcher.group(group));
        }
      }

      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }

    return null;
  }
}
