package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.commons.text.WordUtils;

import java.util.List;
import java.util.function.Function;
import java.util.regex.MatchResult;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class NormalizeText extends Stage {

  private enum NormalizationMode {
    LOWERCASE, UPPERCASE, TITLE_CASE, SENTENCE_CASE, NONE
  }

  private final List<String> sourceFields;
  private final List<String> destFields;
  private String mode;

  private Function<String, String> func;

  public NormalizeText(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.mode = config.getString("mode");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Normalize Text");
    StageUtils.validateFieldNumNotZero(destFields, "Normalize Text");
    StageUtils.validateFieldNumsOneToSeveral(sourceFields, destFields, "Normalize Text");

    switch (mode.toLowerCase()) {
      case ("lowercase"):
        func = this::lowercaseNormalize;
        break;
      case ("uppercase"):
        func = this::uppercaseNormalize;
        break;
      case ("title_case"):
        func = this::titleCaseNormalize;
        break;
      case ("sentence_case"):
        func = this::sentenceCaseNormalize;
        break;
      default:
        // TODO : Should we throw an exception if an invalid mode is supplied or do nothing to the Strings?
        func = this::noOp;
    }
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      for (String value : doc.getStringList(sourceField)) {
        doc.addToField(destField, func.apply(value));
      }
    }

    return null;
  }

  /**
   * Transform the given String to all lowercase
   *
   * @param value The input String
   * @return  lowercased String
   */
  private String lowercaseNormalize(String value) {
    return value.toLowerCase();
  }

  /**
   * Transform the given String to all uppercase
   *
   * @param value The input String
   * @return  uppercased String
   */
  private String uppercaseNormalize(String value) {
    return value.toLowerCase();
  }

  /**
   * Transform the given String into title case ex: "This Is A Title Cased String"
   *
   * @param value the input String
   * @return  the title cased String
   */
  private String titleCaseNormalize(String value) {
    return WordUtils.capitalizeFully(value);
  }

  /**
   * Transform the given String into sentence case ex: "This string is a sentence. Is this a new sentence? One more!"
   *
   * @param value the input String
   * @return  the sentence cased String
   */
  // TODO : Discuss limitations of this/any design
  private String sentenceCaseNormalize(String value) {
    Pattern pattern = Pattern.compile("[.?!] \\w");
    Matcher matcher = pattern.matcher(value.toLowerCase());

    // For each pattern match, replace the first letter of the word with
    String out = matcher.replaceAll(result -> {
      StringBuilder builder = new StringBuilder(result.group());
      builder.replace(builder.length() - 1, builder.length(),
          String.valueOf(Character.toUpperCase(builder.charAt(builder.length() - 1))));
      return builder.toString();
    });

    // Change the first character in the output String to be uppercase
    char upper = Character.toUpperCase(out.charAt(0));
    return upper + out.substring(1);
  }

  /**
   * Do nothing to the String. This noOp allows us to continue running if the user supplies an invalid mode.
   *
   * @param value the input String
   * @return  the unchanged String
   */
  private String noOp(String value) {
    return value;
  }

}
