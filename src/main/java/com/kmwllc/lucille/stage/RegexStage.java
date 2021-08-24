package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This stage supports performing text extraction based on a given Java regex expression. You can supply a comma
 * separated list of fields to apply the text extraction to multiple fields. Extracted values are added to the field on
 * top of the existing field value.
 */
public class RegexStage extends Stage {

  private final String SOURCE_FIELDS_STR;
  private final String DEST_FIELDS_STR;
  private final String REGEX_EXPR;

  public RegexStage(Config config) {
    super(config);
    this.SOURCE_FIELDS_STR = config.getString("source");
    this.DEST_FIELDS_STR = config.getString("dest");
    this.REGEX_EXPR = config.getString("regex");
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    Pattern pattern = Pattern.compile(REGEX_EXPR);

    String[] srcFields = SOURCE_FIELDS_STR.split(",");
    String[] destFields = DEST_FIELDS_STR.split(",");

    StageUtil.validateFieldNumNotZero(SOURCE_FIELDS_STR, "Regex Stage");
    StageUtil.validateFieldNumNotZero(DEST_FIELDS_STR, "Regex Stage");
    StageUtil.validateFieldNumsOneToSeveral(SOURCE_FIELDS_STR, DEST_FIELDS_STR, "Regex Stage");

    int numFields = Integer.max(destFields.length, srcFields.length);

    for (int i = 0; i < numFields; i++) {
      String sourceField = srcFields.length == 1 ? srcFields[0] : srcFields[i];
      String destField = destFields.length == 1 ? destFields[0] : destFields[i];

      if (!doc.has(sourceField))
        continue;

      Matcher matcher = pattern.matcher(doc.getString(sourceField));

      while (matcher.find()) {
        doc.addToField(destField, matcher.group());
      }
    }

    return null;
  }
}
