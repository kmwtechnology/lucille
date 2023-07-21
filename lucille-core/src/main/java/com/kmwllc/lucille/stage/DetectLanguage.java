package com.kmwllc.lucille.stage;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.Language;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Iterator;
import java.util.List;

/**
 * Detects the language of the text in each supplied source field and outputs the language
 * abbreviation associated with the text to the destination fields.
 *
 * <p>Config Parameters:
 *
 * <p>- source (List<String>) : List of source field names. - dest (List<String>) : List of
 * destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be
 * mapped into. - min_length (Integer) : The min length of Strings to be considered for language
 * detection. Shorter Strings will be ignored. - max_length (Integer) : The max length of Strings to
 * be considered for language detection. Longer Strings will be truncated. - min_probability
 * (Double) : The min probability for a language result to be considered valid. Results below this
 * threshold will be ignored.
 */
public class DetectLanguage extends Stage {

  private static final String profileResourcesLoc = "profiles";

  private static final String[] profiles = {
    "af", "ar", "bg", "bn", "cs", "da", "de", "el", "en", "es", "et", "fa", "fi", "fr", "gu", "he",
    "hi", "hr", "hu", "id", "it", "ja", "kn", "ko", "lt", "lv", "mk", "ml", "mr", "ne", "nl", "no",
    "pa", "pl", "pt", "ro", "ru", "sk", "sl", "so", "sq", "sv", "sw", "ta", "te", "th", "tl", "tr",
    "uk", "ur", "vi", "zh-cn", "zh-tw"
  };

  private final List<String> sourceFields;
  private final String languageField;
  private final String languageConfidenceField;
  private final int minLength;
  private final int maxLength;
  private final double minProbability;
  private final UpdateMode updateMode;

  private Detector detector;

  public DetectLanguage(Config config) {
    super(
        config,
        new StageSpec()
            .withRequiredProperties("source", "language_field")
            .withOptionalProperties(
                "language_confidence_field",
                "min_length",
                "max_length",
                "min_probability",
                "update_mode"));

    this.sourceFields = config.getStringList("source");
    this.languageField = config.getString("language_field");
    this.languageConfidenceField =
        config.hasPath("language_confidence_field")
            ? config.getString("language_confidence_field")
            : "languageConfidence";
    this.minLength = config.hasPath("min_length") ? config.getInt("min_length") : 50;
    this.maxLength = config.hasPath("max_length") ? config.getInt("max_length") : 10_000;
    this.minProbability =
        config.hasPath("min_probability") ? config.getDouble("min_probability") : .95;
    this.updateMode = UpdateMode.fromConfig(config);
  }

  private static synchronized void copyResources(File profDir, String profilesPath)
      throws StageException {
    // If the profiles directory does not exist, try to create it.
    if (!profDir.exists()) {
      if (!profDir.mkdirs()) {
        throw new StageException(
            "Unable to create profiles directory for storing Language Detection profiles.");
      }

      // Copy the profiles from the classpath resources to the local file system.
      try {
        for (String profile : profiles) {
          InputStream profileStream =
              DetectLanguage.class
                  .getClassLoader()
                  .getResourceAsStream(profileResourcesLoc + "/" + profile);
          File profFile = new File(profilesPath + "/" + profile);
          Files.copy(profileStream, profFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
          profileStream.close();
        }
      } catch (Exception e) {
        throw new StageException("Unable to copy profiles from resources to local file system.", e);
      }
    }
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Detect Language");

    if (!DetectorFactory.getLangList().isEmpty()) {
      return;
    }

    String profilesPath = FileUtils.getLucilleHomeDirectory() + "/DetectLanguage/profiles";
    File profileDir = new File(profilesPath);

    copyResources(profileDir, profilesPath);

    // Load the profiles
    try {
      DetectorFactory.loadProfile(profileDir);
    } catch (Exception e) {
      throw new StageException("Unable to load language profiles", e);
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      detector = DetectorFactory.create();
      detector.setMaxTextLength(maxLength);
    } catch (Exception e) {
      throw new StageException("Unable to create new Language Detector", e);
    }

    StringBuilder builder = new StringBuilder();
    for (String source : sourceFields) {

      if (!doc.has(source)) continue;

      for (String value : doc.getStringList(source)) {
        builder.append(value);

        if (builder.length() > maxLength) break;
      }
    }

    if (builder.length() < minLength) return null;

    try {
      detector.append(builder.substring(0, Math.min(builder.length(), maxLength)));
      Language result = detector.getProbabilities().get(0);

      if (result.prob >= minProbability) {
        doc.update(languageField, updateMode, result.lang);
        doc.update(languageConfidenceField, updateMode, Math.floor(result.prob * 100) / 100);
      }
    } catch (Exception e) {
      throw new StageException("Unable to detect language", e);
    }

    return null;
  }
}
