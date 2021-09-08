package com.kmwllc.lucille.stage;

import com.cybozu.labs.langdetect.Detector;
import com.cybozu.labs.langdetect.DetectorFactory;
import com.cybozu.labs.langdetect.Language;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * This Stage will detect the langauge of the text in each supplied source field, for each document and output the
 * language abbreviation associated with the text to the destination fields.
 * <p>
 * Config Parameters:
 * <p>
 * - source (List<String>) : List of source field names.
 * - dest (List<String>) : List of destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * - profiles_dir : The path to the directory containing language profiles for language extraction.
 * - min_length : The min length of Strings to be considered for language detection. Shorter Strings will be ignored.
 * - max_length : The max length of Strings to be considered for language detection. Longer Strings will be truncated.
 * - min_probability : The min probability for a language result to be considered valid. Results below this threshold
 * will be ignored.
 */
public class DetectLanguage extends Stage {

  private final static String profileResourcesLoc = "LanguageDetection/profiles";

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String profilesDirStr;
  private final int minLength;
  private final int maxLength;
  private final double minProbability;

  private Detector detector;

  public DetectLanguage(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.profilesDirStr = config.getString("profiles_dir");
    this.minLength = config.getInt("min_length");
    this.maxLength = config.getInt("max_length");
    this.minProbability = config.getDouble("min_probability");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Detect Language");
    StageUtils.validateFieldNumNotZero(destFields, "Detect Language");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Detect Language");

    // TODO : Find a solution for handling the profiles
    File profDirectory;
    try {
      URL url = getClass().getClassLoader().getResource(profileResourcesLoc);
      profDirectory = new File(url.toURI());
    } catch (Exception e) {
      throw new StageException(e.getMessage());
    }

    try {
      DetectorFactory.loadProfile(profDirectory);
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
      String dest = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

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
          doc.addToField(dest, result.lang);
        }

      } catch (Exception e) {
        throw new StageException(e.getMessage());
      }
    }

    return null;
  }
}

/*profDirectory.mkdir();
    Path target = Paths.get(profilesDirStr);

    try {
    URI resource = getClass().getClassLoader().getResource(profileResourcesLoc).toURI();
    FileSystem fileSystem = FileSystems.newFileSystem(
    resource,
    Collections.EMPTY_MAP
    );

final Path jarPath = fileSystem.getPath(profileResourcesLoc);

    Files.walkFileTree(Path.of(profilesDirStr), new SimpleFileVisitor<Path>() {
private Path currentTarget;

@Override
public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
    currentTarget = target.resolve(jarPath.relativize(dir).toString());
    Files.createDirectories(currentTarget);
    return FileVisitResult.CONTINUE;
    }

@Override
public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
    Files.copy(file, target.resolve(jarPath.relativize(file).toString()), StandardCopyOption.REPLACE_EXISTING);
    return FileVisitResult.CONTINUE;
    }
    });
    } catch (Exception e) {
    throw new StageException(e.getMessage());
    }*/
