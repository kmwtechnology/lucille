package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This stage supports performing text extraction based on a given Java regex expression. You can supply a comma
 * separated list of fields to apply the text extraction to multiple fields. Extracted values are added to the field on
 * top of the existing field value.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : List of source field names.
 *   - dest (List<String>) : List of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - regex (String) : A regex expression to find matches for. Matches will be extracted and placed in the destination fields.
 *   - ignore_case (Boolean, Optional) : Determines whether the regex matcher should ignore case.
 *   - multiline (Boolean, Optional) : Determines whether the regex matcher should allow matches across multiple lines.
 *   - dotall (Boolean, Optional) : Turns on the DOTALL functionality for the regex matcher.
 *   - literal (Boolean, Optional) : Toggles treating the regex expression as a literal String.
 */
public class ApplyRegex extends Stage {
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String regexExpr;

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
      case (1) :
        pattern = Pattern.compile(regexExpr, flags.get(0));
        break;
      case (2) :
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1));
        break;
      case (3) :
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2));
        break;
      case (4) :
        pattern = Pattern.compile(regexExpr, flags.get(0) | flags.get(1) | flags.get(2) | flags.get(3));
        break;
      default :
        pattern = Pattern.compile(regexExpr);
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

      for (String value : doc.getStringList(sourceField)) {
        Matcher matcher = pattern.matcher(value);

        // If we find regex matches in the text, add them to the output field
        while (matcher.find()) {
          doc.addToField(destField, matcher.group());
        }
      }
    }

    return null;
  }
}
