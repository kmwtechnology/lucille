package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import org.ahocorasick.trie.PayloadTrie;

import java.util.Iterator;
import java.util.List;

/**
 * Checks if any of the given fields contain any of the given values and tags the given
 * output field with the value.
 * <br>
 * Config Parameters:
 * <p> - contains (List&lt;String&gt;) : A list of values to search for
 * <p> - output (String) : The field to tag if a match is found
 * <p> - value (String) : The value to tag the output field with
 * <p> - ignoreCase (Boolean, Optional) : Determines if the matching should be case insensitive. Defaults to true.
 * <p> - fields (List&lt;String&gt;) : The fields to be searched
 */
public class Contains extends Stage {

  private final List<String> contains;
  private final String output;
  private final String value;
  private final boolean ignoreCase;
  private final List<String> fields;

  private PayloadTrie<String> trie;

  /**
   * Creates the Contains stage from the given Config.
   *
   * @param config Configuration for the Contains stage.
   */
  public Contains(Config config) {
    super(config, Spec.stage()
        .withRequiredProperties("contains", "output", "value", "fields")
        .withOptionalProperties("ignoreCase"));

    this.contains = config.getStringList("contains");
    this.output = config.getString("output");
    this.value = config.getString("value");
    this.ignoreCase = config.hasPath("ignoreCase") ? config.getBoolean("ignoreCase") : true;
    this.fields = config.getStringList("fields");
  }

  @Override
  public void start() throws StageException {
    if (contains.isEmpty()) {
      throw new StageException("Must supply at least one value to check on the field.");
    }

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
  public Iterator<Document> processDocument(Document doc) throws StageException {
    for (String field : fields) {
      if (!doc.has(field)) {
        continue;
      }

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
