package com.kmwllc.lucille.stage;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.Language;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import org.apache.commons.io.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * This Stage will detect the langauge of the text in each supplied source field, for each document and output the
 * language abbreviation associated with the text to the destination fields.
 * <p>
 * Config Parameters:
 * <p>
 * - source (List<String>) : List of source field names.
 * - dest (List<String>) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * - min_length : The min length of Strings to be considered for language detection. Shorter Strings will be ignored.
 * - max_length : The max length of Strings to be considered for language detection. Longer Strings will be truncated.
 * - min_probability : The min probability for a language result to be considered valid. Results below this threshold
 * will be ignored.
 */
public class DetectLanguage extends Stage {

  private final static String profileResourcesLoc = "profiles";

  private final static String[] profiles = {"af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa",
      "fi", "fr", "gu", "he", "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr", "ne", "nl",
      "no", "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr", "uk", "ur",
      "vi", "zh-cn", "zh-tw"};

  private final List<String> sourceFields;
  private final String languageField;
  private final String languageConfidenceField;
  private final int minLength;
  private final int maxLength;
  private final double minProbability;

  private Detector detector;

  public DetectLanguage(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.languageField = config.getString("language_field");
    this.languageConfidenceField = config.getString("language_confidence_field");
    this.minLength = config.getInt("min_length");
    this.maxLength = config.getInt("max_length");
    this.minProbability = config.getDouble("min_probability");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Detect Language");

    String profilesPath = "LUCILLE_HOME/DetectLanguage/profiles";
    File profileDir = new File(profilesPath);

    if (!profileDir.exists()) {
      if (!profileDir.mkdirs()) {
        throw new StageException("Unable to create profiles directory for storing Language Detection profiles.");
      }

      try {
        for (String profile : profiles) {
          InputStream profileStream = getClass().getClassLoader().getResourceAsStream(profileResourcesLoc + "/" + profile);
          FileUtils.copyInputStreamToFile(profileStream, new File(profilesPath + "/" + profile));
        }
      } catch (Exception e) {
        throw new StageException(e.getMessage());
      }
    }

    try {
      DetectorFactory.loadProfile(profileDir);
    } catch (Exception e) {
      throw new StageException(e.getMessage());
    }
  }


  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      StringBuilder builder = new StringBuilder();
      try {
        detector = DetectorFactory.create();
        detector.setMaxTextLength(maxLength);
      } catch (Exception e) {
        throw new StageException(e.getMessage());
      }

      String source = sourceFields.get(i);

      if (!doc.has(source))
        continue;

      for (String value : doc.getStringList(source)) {
        builder.append(value);
      }

      if (builder.length() < minLength)
        continue;

      try {
        detector.append(builder.toString());
        Language result = detector.getProbabilities().get(0);

        if (result.prob >= minProbability) {
          doc.setField(languageField, result.lang);
          doc.setField(languageConfidenceField, (int) result.prob * 100);
        }

      } catch (Exception e) {
        throw new StageException(e.getMessage());
      }
    }

    return null;
  }
}