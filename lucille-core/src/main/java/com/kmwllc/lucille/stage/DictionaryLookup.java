package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.spec.Spec;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <p> Finds exact matches for given input values and extracts the payloads for each match to a given destination field.
 * The dictionary file should have a term on each line, and can support providing payloads with
 * the syntax "term, payload". If any occurrences are found, they will be extracted and their associated payloads will
 * be appended to the destination field.
 *
 * <p> Can also be used as a Set lookup by setting the set_only parameter to true. In this case, the destination field will
 * be set to true if all values in the source field are present in the dictionary.
 *
 * <p> Config Parameters:
 * <p>  - source (List&lt;String&gt;) : list of source field names
 * <p>  - dest (List&lt;String&gt;) : list of destination field names. You can either supply the same number of source and destination fields
 *       for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.
 * <p>  - dict_path (String) : The path the dictionary to use for matching. If the dict_path begins with "classpath:" the classpath
 *       will be searched for the file. Otherwise, the local file system will be searched.
 * <p>  - use_payloads (Boolean, Optional) : denotes whether payloads from the dictionary should be used or not. Defaults to true.
 * <p>  - update_mode (String, Optional) : Determines how writing will be handling if the destination field is already populated.
 *      Can be 'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.
 * <p>  - set_only (Boolean, Optional) : If true, the destination field will be set to true when a match is found (instead of
 *      outputting the match itself). You can control this behavior with <code>use_any_match</code> and <code>ignore_missing_source</code>.
 *      <code>set_only</code> defaults to false.
 * <p>  - use_any_match (Boolean, Optional) : Use in combination with set_only. If true, the destination field will
 *      be set to true if any values in any source field are present in the dictionary. If false, the destination field
 *      will be set to true if all the values in each source field are present in the dictionary. Defaults to false.
 * <p>  - ignore_missing_source (Boolean, Optional) : Use in combination with set_only. If true, the destination field
 *      will be set to true if the source field is missing. Defaults to false.
 *
 * <p>  - s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.
 * <p>  - azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.
 * <p>  - gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.
 */
public class DictionaryLookup extends Stage {

  public static final Spec SPEC = Spec.stage()
      .requiredList("source", new TypeReference<List<String>>(){})
      .requiredList("dest", new TypeReference<List<String>>(){})
      .requiredString("dict_path")
      .optionalBoolean("use_payloads", "ignore_case", "set_only", "use_any_match", "ignore_missing_source")
      .optionalString("update_mode")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC);

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final String dictPath;
  private final boolean usePayloads;
  private final UpdateMode updateMode;
  private final boolean ignoreCase;
  private final boolean setOnly;
  private final boolean useAnyMatch;
  private final boolean ignoreMissingSource;

  private Map<String, List<String>> dict;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DictionaryLookup(Config config) throws StageException {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.updateMode = UpdateMode.fromConfig(config);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.setOnly = ConfigUtils.getOrDefault(config, "set_only", false);
    this.useAnyMatch = ConfigUtils.getOrDefault(config, "use_any_match", false);
    this.ignoreMissingSource = ConfigUtils.getOrDefault(config, "ignore_missing_source", false);
    this.dictPath = config.getString("dict_path");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Dictionary Lookup");
    StageUtils.validateFieldNumNotZero(destFields, "Dictionary Lookup");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Dictionary Lookup");

    if (config.hasPath("ignore_missing_source") && !setOnly) {
      log.warn("ignore_missing_source is only valid when set_only is true. Ignoring.");
    }

    if (config.hasPath("use_any_match") && !setOnly) {
      log.warn("use_any_match is only valid when set_only is true. Ignoring.");
    }

    if (setOnly && updateMode != UpdateMode.OVERWRITE) {
      throw new StageException("when set_only is true, update_mode must be set to overwrite");
    }

    this.dict = DictionaryManager.getDictionary(dictPath, ignoreCase, setOnly, config);
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

        boolean currentValue = false;
        if (!doc.has(sourceField)) {
          // if ignoreMissingSource is true, set the destination field to true if the source field is missing
          currentValue = ignoreMissingSource;
        } else {
          Stream<String> sourceStream = doc.getStringList(sourceField).stream()
              .map(ignoreCase ? String::toLowerCase : String::toString);

          if (useAnyMatch) {
            // once we get a match on any field, can break the loop and just update the field to true.
            if (sourceStream.anyMatch(dict::containsKey)) {
              doc.update(destField, updateMode, true);
              return null;
            }
          } else {
            currentValue = sourceStream.allMatch(dict::containsKey);
          }
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
              outputValues.addAll(dict.get(value));
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
