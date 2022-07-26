package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.JsonDocument;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.ahocorasick.trie.PayloadTrie;

import java.util.List;

/**
 * Checks if any of the given fields contain any of the given values and tags the given
 * output field with the value.
 *
 * Config Parameters:
 *
 *   - contains (List<String>) : A list of values to search for
 *   - output (String) : The field to tag if a match is found
 *   - value (String) : The value to tag the output field with
 *   - ignoreCase (Boolean, Optional) : Determines if the matching should be case insensitive. Defaults to true.
 *   - field (List<String>) : The fields to be searched
 */
public class Contains extends Stage {

  private final List<String> contains;
  private final String output;
  private final String value;
  private final boolean ignoreCase;
  private final List<String> fields;

  private PayloadTrie<String> trie;

  public Contains(Config config) {
    super(config);
    this.contains = config.getStringList("contains");
    this.output = config.getString("output");
    this.value = config.getString("value");
    this.ignoreCase = config.hasPath("ignoreCase") ? config.getBoolean("ignoreCase") : true;
    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (contains.isEmpty())
      throw new StageException("Must supply at least one value to check on the field.");

    trie = buildTrie();
  }

  private PayloadTrie<String> buildTrie() {
    PayloadTrie.PayloadTrieBuilder<String> trieBuilder = PayloadTrie.builder();

    // For each of the possible Trie settings
    trieBuilder = trieBuilder.stopOnHit();
    trieBuilder = trieBuilder.onlyWholeWords();
    if (ignoreCase) {
      trieBuilder = trieBuilder.ignoreCase();
    }

    for (String val : contains) {
      trieBuilder = trieBuilder.addKeyword(val, val);
    }

    return trieBuilder.build();
  }

  @Override
  public List<JsonDocument> processDocument(JsonDocument doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field))
        continue;

      List<String> values = doc.getStringList(field);
      for (String value : values) {
        if (trie.containsMatch(value)) {
          doc.update(output, UpdateMode.DEFAULT, this.value);
          return null;
        }
      }
    }

    return null;
  }
}
