package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.StageUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import com.typesafe.config.Config;
import java.io.BufferedReader;
import java.io.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * Finds exact matches for given input values and extracts the payloads for each match to a given destination field.
 * The dictionary file should have a term on each line, and can support providing payloads with
 * the syntax "term, payload". If any occurrences are found, they will be extracted and their associated payloads will
 * be appended to the destination field.
 *
 * Can also be used as a Set lookup by setting the set_only parameter to true. In this case, the destination field will
 * be set to true if all values in the source field are present in the dictionary.
 *
 * Config Parameters:
 *
 *   - source (List<String>) : list of source field names
 *   - dest (List<String>) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - dict_path (String) : The path the dictionary to use for matching. If the dict_path begins with "classpath:" the classpath
 *       will be searched for the file. Otherwise, the local file system will be searched.
 *   - use_payloads (Boolean, Optional) : denotes whether paylaods from the dictionary should be used or not. Defaults to true.
 *   - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 *   - set_only (Boolean, Optional) : If true, the destination field will be set to true if all values in the source field
 *      are present in the dictionary.
 *   - ignore_missing_source (Boolean, Optional) : Intended to be used in combination with set_only. If true, the destination field
 *      will be set to true if the source field is missing. Defaults to false.
 */
public class DictionaryLookup extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final HashMap<String, String[]> dict;
  private final boolean usePayloads;
  private final UpdateMode updateMode;
  private final boolean ignoreCase;
  private final boolean setOnly;
  private final boolean ignoreMissingSource;

  // Dummy value to indicate that a key is present in the HashMap
  private static final String[] PRESENT = new String[0];
  private static final Logger LOG = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DictionaryLookup(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source", "dest", "dict_path")
        .withOptionalProperties("use_payloads", "update_mode", "ignore_case", "set_only", "ignore_missing_source"));

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.updateMode = UpdateMode.fromConfig(config);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.setOnly = ConfigUtils.getOrDefault(config, "set_only", false);
    this.ignoreMissingSource = ConfigUtils.getOrDefault(config, "ignore_missing_source", false);
    this.dict = buildHashMap(config.getString("dict_path"));
  }

  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Dictionary Lookup");
    StageUtils.validateFieldNumNotZero(destFields, "Dictionary Lookup");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Dictionary Lookup");
    if (ignoreMissingSource && !setOnly) {
      LOG.warn("ignore_missing_source is only valid when set_only is true. Ignoring.");
    }
  }

  /**
   * Create a HashMap matching key phrases from the dictionary to payloads
   *
   * @param dictPath  the path of the dictionary file
   * @return the populated HashMap
   */
  private HashMap<String, String[]> buildHashMap(String dictPath) throws StageException {

    // count lines and create a set with correct capacity
    int lineCount = countLines(dictPath);
    HashMap<String, String[]> dict = new HashMap<>((int) Math.ceil(lineCount / 0.75));

    try (CSVReader reader = new CSVReader(getFileReader(dictPath))) {
      // For each line of the dictionary file, add a keyword/payload pair to the hash map
      String[] line;
      boolean ignore = false;
      while ((line = reader.readNext()) != null) {
        if (line.length == 0) {
          continue;
        }

        for (String term : line) {
          if (term.contains("\uFFFD")) {
            LOG.warn(String.format("Entry \"%s\" on line %d contained malformed characters which were removed. " +
                "This dictionary entry will be ignored.", term, reader.getLinesRead()));
            ignore = true;
            break;
          }
        }

        if (ignore) {
          ignore = false;
          continue;
        }

        // save the first word for both single and multi-word lines
        String word = line[0].trim();

        // TODO : Add log messages for when encoding errors occur so that they can be fixed
        String[] value;
        if (line.length == 1) {
          value = setOnly ? PRESENT : new String[]{word};
        } else if (setOnly) {
          LOG.warn(String.format("Entry \"%s\" on line %d contained payloads which were ignored. " +
              "This dictionary entry will be treated as a set.", word, reader.getLinesRead()));
          value = PRESENT;
        } else {
          // Handle multiple payload values here.
          value = Arrays.stream(Arrays.copyOfRange(line, 1, line.length)).map(String::trim).toArray(String[]::new);
        }
        // Add the word and its payload(s) to the dictionary
        dict.put(ignoreCase ? word.toLowerCase() : word, value);
      }
    } catch (IOException e) {
      throw new StageException("Failed to read from the given file.", e);
    } catch (CsvValidationException e) {
      throw new StageException("Error validating CSV", e);
    }
    return dict;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    // todo consider what should happen if using set and destination field is already populated
    if (setOnly) {
      for (String destField : destFields) {
        doc.removeField(destField);
      }
    }

    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField)) {
        if (setOnly) {
          boolean currentValue = !doc.has(destField) || doc.getBoolean(destField);
          doc.setField(destField, currentValue && ignoreMissingSource);
        }
        continue;
      }

      if (setOnly) {
        // check if all values in the source field are in the dictionary
        boolean currentValue = !doc.has(destField) || doc.getBoolean(destField);
        doc.setField(destField, currentValue && doc.getStringList(sourceField).stream()
            .map(ignoreCase ? String::toLowerCase : String::toString)
            .allMatch(dict::containsKey));
        continue;
      }

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        if (ignoreCase) {
          value = value.toLowerCase();
        }
        if (dict.containsKey(value)) {
          if (usePayloads) {
            outputValues.addAll(Arrays.asList(dict.get(value)));
          } else {
            outputValues.add(value);
          }
        }
      }

      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }

    return null;
  }

  /**
   * Get a Reader for the given path.
   * todo consider moving this method to a utility class
   *
   * @param path file path
   * @return Reader object
   * @throws StageException if the file does not exist or cannot be read
   */
  private static Reader getFileReader(String path) throws StageException {
    try {
      return FileUtils.getReader(path);
    } catch (NullPointerException | IOException e) {
      throw new StageException("File does not exist: " + path);
    }
  }

  /**
   * Count the number of lines in a file.
   * todo consider moving this method to a utility class
   *
   * @param filename file path
   * @return number of lines
   * @throws StageException if the file does not exist or cannot be read
   */
  private static int countLines(String filename) throws StageException {
    try (BufferedReader reader = new BufferedReader(getFileReader(filename))) {
      int lines = 0;
      while (reader.readLine() != null) {
        lines++;
      }
      return lines;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
