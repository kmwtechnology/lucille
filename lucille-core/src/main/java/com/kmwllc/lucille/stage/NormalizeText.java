package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import org.apache.commons.text.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Provides 4 modes for normalizing the case of text: Lowercase, Uppercase, Title Case and Sentence Case.
 * The desired mode should be set in the configuration file. NOTE: This stage will not preserve any capitalization from
 * the original document. As such, proper nouns, abbreviations and acronyms may not be correctly capitalized after
 * normalization.
 * Config Parameters:
 *
 *   - source (List&lt;String&gt;) : List of source field names.
 *   - dest (List&lt;String&gt;) : List of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - mode (String) : The mode for normalization: uppercase, lowercase, sentence_case, title_case.
 *   - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 */
public class NormalizeText extends Stage {

  public static final Spec SPEC = Spec.stage()
      .requiredList("source", new TypeReference<List<String>>(){})
      .requiredList("dest", new TypeReference<List<String>>(){})
      .requiredString("mode")
      .optionalString("update_mode");

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  private static final Pattern pattern = Pattern.compile("[.?!] \\w");

  private final List<String> sourceFields;
  private final List<String> destFields;
  private String mode;
  private final UpdateMode updateMode;

  private Function<String, String> func;

  /**
   *
   * @param config
   */
  public NormalizeText(Config config) {
    super(config);
    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.mode = config.getString("mode");
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Normalize Text");
    StageUtils.validateFieldNumNotZero(destFields, "Normalize Text");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Normalize Text");

    this.mode = mode;

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
        throw new StageException("Invalid Mode supplied to NormalizeText.");
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField)) {
        continue;
      }

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        outputValues.add(func.apply(value));
      }

      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }

    return null;
  }

  /**
   * Transform the given String to all lowercase
   *
   * @param value The input String
   * @return lowercased String
   */
  private String lowercaseNormalize(String value) {
    return value.toLowerCase();
  }

  /**
   * Transform the given String to all uppercase
   *
   * @param value The input String
   * @return uppercased String
   */
  private String uppercaseNormalize(String value) {
    return value.toUpperCase();
  }

  /**
   * Transform the given String into title case ex: "This Is A Title Cased String"
   * This normalization will put ALL of the text in title case and will not preserve the case of things such as
   * abbreviations or fully capitalized words. This can handle non latin languages.
   *
   * @param value the input String
   * @return the title cased String
   */
  private String titleCaseNormalize(String value) {
    return WordUtils.capitalizeFully(value);
  }

  /**
   * Transform the given String into sentence case ex: "This string is a sentence. Is this a new sentence? One more!"
   * This normalization will put ALL of the text in title case and will not preserve the case of things such a
   * abbreviations, fully capitalized words or proper nouns.
   *
   * @param value the input String
   * @return the sentence cased String
   */
  private String sentenceCaseNormalize(String value) {
    // TODO : decide if we want to preserve original capitalization
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
}
