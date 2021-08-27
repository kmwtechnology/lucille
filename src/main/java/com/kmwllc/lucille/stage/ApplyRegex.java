package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This stage supports performing text extraction based on a given Java regex expression. You can supply a comma
 * separated list of fields to apply the text extraction to multiple fields. Extracted values are added to the field on
 * top of the existing field value.
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
  private int numFields;

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
    StageUtils.validateFieldNumsOneToSeveral(sourceFields, destFields, "Apply Regex");
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

        while (matcher.find()) {
          doc.addToField(destField, matcher.group());
        }
      }
    }

    return null;
  }
}
