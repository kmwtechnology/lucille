package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.StageUtils;
import com.kmwllc.lucille.core.UpdateMode;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderHeaderAware;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * This stage supports finding exact matches for given input values, and extracting the payloads for each match to a
 * given destination field. The dictionary file should have a term on each line, and can support providing payloads with
 * the syntax "term, payload". If any occurrences are found, they will be extracted and their associated payloads will
 * be appended to the destination field.
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
 */
public class DictionaryLookup extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final HashMap<String, String> dict;
  private final boolean usePayloads;
  private final UpdateMode updateMode;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DictionaryLookup(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.dict = buildHashMap(config.getString("dict_path"));
    this.usePayloads = StageUtils.configGetOrDefault(config, "use_payloads" ,true);
    this.updateMode = UpdateMode.fromConfig(config);
  }

  /**
   * Create a HashMap matching key phrases from the dictionary to payloads
   *
   * @param dictPath  the path of the dictionary file
   * @return  the populated HashMap
   */
  private HashMap<String, String> buildHashMap(String dictPath) {
    HashMap<String, String> dict = new HashMap<>();
    try (CSVReader reader = new CSVReader(FileUtils.getReader(dictPath))) {
      // For each line of the dictionary file, add a keyword/payload pair to the Trie
      String[] line;
      boolean ignore = false;
      while((line = reader.readNext()) != null) {
        if (line.length == 0)
          continue;

        for (String term : line) {
          if (term.contains("\uFFFD")) {
            log.warn(String.format("Entry \"%s\" on line %d contained malformed characters which were removed. " +
                "This dictionary entry will be ignored.", term, reader.getLinesRead()));
            ignore = true;
            break;
          }
        }

        if (ignore) {
          ignore = false;
          continue;
        }

        // TODO : Add log messages for when encoding errors occur so that they can be fixed
        // TODO : Multiple payloads (eventually)
        if (line.length == 1) {
          String word = line[0].trim();
          dict.put(word, word);
        } else {
          dict.put(line[0].trim(), line[1].trim());
        }
      }
    } catch (Exception e) {
      log.error("Failed to read from the given file.", e);
    }

    return dict;
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      List<String> outputValues = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        if (dict.containsKey(value)) {
          if (usePayloads) {
            outputValues.add(dict.get(value));
          } else {
            outputValues.add(value);
          }
        }
      }

      doc.update(destField, updateMode, outputValues.toArray(new String[0]));
    }

    return null;
  }
}
