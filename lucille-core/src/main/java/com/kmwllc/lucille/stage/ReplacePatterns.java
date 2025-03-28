package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.core.configSpec.StageSpec;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Replaces any of the given Patterns found in the source fields with a given replacement String.
 * <br>
 * Config Parameters:
 * <br>
 * - source (List&lt;String&gt;) : List of source field names.
 * <br>
 * - dest (List&lt;String&gt;) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * <br>
 * - regex (List&lt;String&gt;) : A list regex expression to find matches for. Matches will be extracted and placed in the destination fields.
 * <br>
 * - replacement (String, Optional) : The String to replace regex matches with. If null, pattern replacement will only take place if
 * a replacement_field is specified and set to a String in a document.
 * <br>
 * - replacement_field (String, Optional): Specify a field in the document that is set to a String. If non-null, replacements of a
 * pattern within a document will use the string set to the replacement_field, if present. Otherwise, we fall back onto replacement:
 * If it is not null, patterns are replaced with it; if it is null, no replacement takes place.
 * <br>
 * - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 * Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 * <br>
 * - ignore_case (Boolean, Optional) : Determines whether the regex matcher should ignore case. Defaults to false.
 * <br>
 * - multiline (Boolean, Optional) : Determines whether the regex matcher should allow matches across multiple lines. Defaults to false.
 * <br>
 * - dotall (Boolean, Optional) : Turns on the DOTALL functionality for the regex matcher. Defaults to false.
 * <br>
 * - literal (Boolean, Optional) : Toggles treating the regex expression as a literal String. Defaults to false.
 */
public class ReplacePatterns extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final List<String> regexExprs;
  private final String replacement;
  private final String replacementField;
  private final UpdateMode updateMode;

  private final boolean ignoreCase;
  private final boolean multiline;
  private final boolean dotall;
  private final boolean literal;

  private List<Pattern> patterns;

  public ReplacePatterns(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source", "dest", "regex")
        .withOptionalProperties("replacement", "replacement_field", "update_mode", "ignore_case", "multiline", "dotall", "literal"));

    this.patterns = new ArrayList<>();

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.regexExprs = config.getStringList("regex");
    this.replacement = ConfigUtils.getOrDefault(config, "replacement", null);
    this.replacementField = ConfigUtils.getOrDefault(config, "replacement_field", null);

    if (replacement == null && replacementField == null) {
      throw new StageException("Did not provide a replacement String or a replacement_field.");
    }

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
        for (String regexExpr : regexExprs) {
          patterns.add(Pattern.compile(regexExpr, flags.get(0)));
        }
        break;
      case (2):
        for (String regexExpr : regexExprs) {
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1)));
        }
        break;
      case (3):
        for (String regexExpr : regexExprs) {
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2)));
        }
        break;
      case (4):
        for (String regexExpr : regexExprs) {
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2) | flags.get(3)));
        }
        break;
      default:
        for (String regexExpr : regexExprs) {
          patterns.add(Pattern.compile(regexExpr));
        }
        break;
    }

    StageUtils.validateFieldNumNotZero(sourceFields, "Apply Regex");
    StageUtils.validateFieldNumNotZero(destFields, "Apply Regex");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Apply Regex");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    String replacementForDoc = doc.getString(replacementField) == null ? replacement : doc.getString(replacementField);

    if (replacementForDoc == null) {
      return null;
    }

    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField)) {
        continue;
      }

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        for (Pattern pattern : patterns) {
          Matcher matcher = pattern.matcher(value);
          value = matcher.replaceAll(replacementForDoc);
        }

        outputValues.add(value);
      }
      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }

    return null;
  }
}
