package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.kmwllc.lucille.util.StageUtils.WriteMode;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Config Parameters:
 * <p>
 * - source (List<String>) : List of source field names.
 * - dest (List<String>) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * - regex (List<String>) : A list regex expression to find matches for. Matches will be extracted and placed in the destination fields.
 * - replacement (String) : The String to replace regex matches with.
 * - write_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 * Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 * - ignore_case (Boolean, Optional) : Determines whether the regex matcher should ignore case. Defaults to false.
 * - multiline (Boolean, Optional) : Determines whether the regex matcher should allow matches across multiple lines. Defaults to false.
 * - dotall (Boolean, Optional) : Turns on the DOTALL functionality for the regex matcher. Defaults to false.
 * - literal (Boolean, Optional) : Toggles treating the regex expression as a literal String. Defaults to false.
 */
public class ReplacePatterns extends Stage {
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final List<String> regexExprs;
  private final String replacement;
  private final WriteMode writeMode;

  private final boolean ignoreCase;
  private final boolean multiline;
  private final boolean dotall;
  private final boolean literal;

  private List<Pattern> patterns;

  public ReplacePatterns(Config config) {
    super(config);

    this.patterns = new ArrayList<>();

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.regexExprs = config.getStringList("regex");
    this.replacement = config.getString("replacement");
    this.writeMode = StageUtils.getWriteMode(StageUtils.configGetOrDefault(config, "write_mode", "overwrite"));

    this.ignoreCase = StageUtils.configGetOrDefault(config, "ignore_case", false);
    this.multiline = StageUtils.configGetOrDefault(config, "multiline", false);
    this.dotall = StageUtils.configGetOrDefault(config, "dotall", false);
    this.literal = StageUtils.configGetOrDefault(config, "literal", false);
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
        for (String regexExpr : regexExprs)
          patterns.add(Pattern.compile(regexExpr, flags.get(0)));
        break;
      case (2):
        for (String regexExpr : regexExprs)
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1)));
        break;
      case (3):
        for (String regexExpr : regexExprs)
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2)));
        break;
      case (4):
        for (String regexExpr : regexExprs)
          patterns.add(Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2) | flags.get(3)));
        break;
      default:
        for (String regexExpr : regexExprs)
          patterns.add(Pattern.compile(regexExpr));
        break;
    }

    StageUtils.validateFieldNumNotZero(sourceFields, "Apply Regex");
    StageUtils.validateFieldNumNotZero(destFields, "Apply Regex");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Apply Regex");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        for (Pattern pattern : patterns) {
          Matcher matcher = pattern.matcher(value);
          value = matcher.replaceAll(replacement);
        }

        outputValues.add(value);
      }
      doc.writeToField(destField, writeMode, outputValues.toArray(new String[0]));
    }

    return null;
  }
}
