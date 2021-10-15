package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.ahocorasick.trie.PayloadTrie;

import java.util.List;

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
    this.ignoreCase = config.getBoolean("ignoreCase");
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
  public List<Document> processDocument(Document doc) throws StageException {
    foundMatch:
    for (String field : fields) {
      if (!doc.has(field))
        continue;

      List<String> values = doc.getStringList(field);
      for (String value : values) {
        if (trie.containsMatch(value)) {
          doc.update(output, UpdateMode.DEFAULT, this.value);
          break foundMatch;
        }
      }
    }

    return null;
  }
}
