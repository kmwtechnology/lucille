package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.invoke.MethodHandles;
import java.util.HashMap;
import java.util.List;

public class DictionaryLookup extends Stage {

  private final List<String> sourceFields;
  private final List<String> destFields;
  private final HashMap<String, String> dict;
  private final boolean usePayloads;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  public DictionaryLookup(Config config) {
    super(config);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.dict = buildHashMap(config.getString("dict_path"));
    this.usePayloads = StageUtils.configGetOrDefault(config, "use_payloads" ,true);
  }

  /**
   * Create a HashMap matching key phrases from the dictionary to payloads
   *
   * @param dictPath  the path of the dictionary file
   * @return  the populated HashMap
   */
  private HashMap<String, String> buildHashMap(String dictPath) {
    HashMap<String, String> dict = new HashMap<>();
    try (BufferedReader reader = new BufferedReader(new FileReader(dictPath))) {
      // For each line of the dictionary file, add a keyword/payload pair to the Trie
      String line;
      while((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;

        String[] keyword = line.split(",");

        if (keyword.length == 1) {
          String word = keyword[0].trim();
          dict.put(word, word);
        } else {
          dict.put(keyword[0].trim(), keyword[1].trim());
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

      for (String value : doc.getStringList(sourceField)) {
        if (dict.containsKey(value)) {
          if (usePayloads) {
            doc.addToField(destField, dict.get(value));
          } else {
            doc.addToField(destField, value);
          }
        }
      }
    }

    return null;
  }
}
