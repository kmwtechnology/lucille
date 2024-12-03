package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.stage.util.DictionaryManager;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Finds exact matches for given input values and extracts the payloads for each match to a given destination field.
 * The dictionary file should have a term on each line, and can support providing payloads with
 * the syntax "term, payload". If any occurrences are found, they will be extracted and their associated payloads will
 * be appended to the destination field.
 *
 * Can also be used as a Set lookup by setting the setOnly parameter to true. In this case, the destination field will
 * be set to true if all values in the source field are present in the dictionary.
 *
 * Config Parameters:
 *
 *   - source (List&lt;String&gt;) : list of source field names
 *   - dest (List&lt;String&gt;) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 *   - dictPath (String) : The path the dictionary to use for matching. If the dictPath begins with "classpath:" the classpath
 *       will be searched for the file. Otherwise, the local file system will be searched.
 *   - usePayloads (Boolean, Optional) : denotes whether paylaods from the dictionary should be used or not. Defaults to true.
 *   - updateMode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 *   - setOnly (Boolean, Optional) : If true, the destination field will be set to true if all values in the source field
 *      are present in the dictionary.
 *   - ignoreMissingSource (Boolean, Optional) : Intended to be used in combination with setOnly. If true, the destination field
 *      will be set to true if the source field is missing. Defaults to false.
 */
public class DictionaryLookup extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String dictPath;
  private final boolean usePayloads;
  private final UpdateMode updateMode;
  private final boolean ignoreCase;
  private final boolean setOnly;
  private final boolean ignoreMissingSource;

  private Map<String, String[]> dict;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());
  // Dummy value to indicate that a key is present in the HashMap

  public DictionaryLookup(Config config) throws StageException {
    super(config, new StageSpec().withRequiredProperties("source", "dest", "dictPath")
        .withOptionalProperties("usePayloads", "updateMode", "ignoreCase", "setOnly", "ignoreMissingSource"));

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.usePayloads = ConfigUtils.getOrDefault(config, "usePayloads", true);
    this.updateMode = UpdateMode.fromConfig(config);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignoreCase", false);
    this.setOnly = ConfigUtils.getOrDefault(config, "setOnly", false);
    this.ignoreMissingSource = ConfigUtils.getOrDefault(config, "ignoreMissingSource", false);
    this.dictPath = config.getString("dictPath");
  }

  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Dictionary Lookup");
    StageUtils.validateFieldNumNotZero(destFields, "Dictionary Lookup");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Dictionary Lookup");

    if (ignoreMissingSource && !setOnly) {
      log.warn("ignoreMissingSource is only valid when setOnly is true. Ignoring.");
    }
    if (setOnly && updateMode != UpdateMode.OVERWRITE) {
      throw new StageException("when setOnly is true, updateMode must be set to overwrite");
    }

    this.dict = DictionaryManager.getDictionary(dictPath, ignoreCase, setOnly);
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {

    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (setOnly) {

        // default value is true if this is the first field or if there are multiple fields
        // in case where we have one destination field and multiple source fields we want to retrieve the current value
        boolean defaultValue = true;
        if (i != 0 && destFields.size() == 1) {
          defaultValue = doc.getBoolean(destField);
        }

        boolean currentValue;
        if (!doc.has(sourceField)) {
          // if ignoreMissingSource is true, set the destination field to true if the source field is missing
          currentValue = ignoreMissingSource;
        } else {
          // check if all values in the source field are in the dictionary
          currentValue = doc.getStringList(sourceField).stream()
              .map(ignoreCase ? String::toLowerCase : String::toString)
              .allMatch(dict::containsKey);
        }

        doc.update(destField, updateMode, defaultValue && currentValue);

      } else {

        if (!doc.has(sourceField)) {
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
    }

    return null;
  }
}
