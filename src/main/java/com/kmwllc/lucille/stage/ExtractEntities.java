package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.StageUtils;
import com.typesafe.config.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.ahocorasick.trie.PayloadEmit;
import org.ahocorasick.trie.PayloadTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage for performing entity extraction on a given field, using terms from a given dictionary file. The dictionary
 * file should have a term on each line, and can support providing payloads with the syntax "term, payload". If any
 * occurrences are found, the original value of the field will be deleted and replaced by the term values.
 */
public class ExtractEntities extends Stage {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PayloadTrie<String> dictTrie;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final boolean ignoreCase;
  private final boolean onlyWhitespaceSeparated;
  private final boolean stopOnHit;
  private final boolean onlyWholeWords;
  private final boolean ignoreOverlaps;
  private final boolean usePayloads;

  private int numFields;

  public ExtractEntities(Config config) {
    super(config);

    // For the optional settings, we check if the config has this setting and then what the value is.
    this.ignoreCase = StageUtils.<Boolean>configGetOrDefault(config, "ignore_case", false);
    this.onlyWhitespaceSeparated = StageUtils.<Boolean>configGetOrDefault(config, "only_whitespace_separated", false);
    this.stopOnHit = StageUtils.<Boolean>configGetOrDefault(config, "stop_on_hit", false);
    this.onlyWholeWords = StageUtils.<Boolean>configGetOrDefault(config, "only_whole_words", false);
    this.ignoreOverlaps = StageUtils.<Boolean>configGetOrDefault(config, "ignore_overlaps", false);
    this.usePayloads = StageUtils.<Boolean>configGetOrDefault(config, "use_payloads", true);

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Extract Entities");
    StageUtils.validateFieldNumNotZero(destFields, "Extract Entities");
    StageUtils.validateFieldNumsOneToSeveral(sourceFields, destFields, "Extract Entities");

    dictTrie = buildTrie(config.getString("dict_path"));
  }

  /**
   * Generate a Trie to perform dictionary based entity extraction with
   *
   * @param dictFile  the path of the dictionary file to read from
   * @return  a PayloadTrie capable of finding matches for its dictionary values
   */
  private PayloadTrie<String> buildTrie(String dictFile) {
    PayloadTrie.PayloadTrieBuilder<String> trieBuilder = PayloadTrie.builder();

    // For each of the possible Trie settings, check what value the user set and apply it.
    if (ignoreCase) {
      trieBuilder = trieBuilder.ignoreCase();
    }

    if (onlyWhitespaceSeparated) {
      trieBuilder = trieBuilder.onlyWholeWordsWhiteSpaceSeparated();
    }

    if (stopOnHit) {
      trieBuilder = trieBuilder.stopOnHit();
    }

    if (onlyWholeWords) {
      trieBuilder = trieBuilder.onlyWholeWords();
    }

    if (ignoreOverlaps) {
      trieBuilder = trieBuilder.ignoreOverlaps();
    }

    try (BufferedReader reader = new BufferedReader(FileUtils.getReader(dictFile))) {
      // For each line of the dictionary file, add a keyword/payload pair to the Trie
      String line;
      while((line = reader.readLine()) != null) {
        if (line.isBlank())
          continue;

        String[] keyword = line.split(",");

        if (keyword.length == 1) {
          String word = keyword[0].trim();
          trieBuilder = trieBuilder.addKeyword(word, word);
        } else {
          trieBuilder = trieBuilder.addKeyword(keyword[0].trim(), keyword[1].trim());
        }
      }
    } catch (Exception e) {
      log.error("Failed to read from the given file.", e);
    }

    return trieBuilder.build();
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    // For each of the field names, extract dictionary values from it.
    for (int i = 0; i < sourceFields.size(); i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      if (!doc.has(sourceField))
        continue;

      // Parse the matches and then convert the PayloadEmits into a List of Strings, representing the payloads for
      // each match which occurred in the input string.
      Collection<PayloadEmit<String>> results = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        results.addAll(dictTrie.parseText(value));
      }

      List<String> payloads;
      if (usePayloads) {
        payloads = results.stream().map(PayloadEmit::getPayload).collect(Collectors.toList());
      } else {
        payloads = results.stream().map(PayloadEmit::getKeyword).collect(Collectors.toList());
      }

      if (payloads.isEmpty())
        continue;

      for (String payload : payloads) {
        doc.addToField(destField, payload);
      }
    }

    return null;
  }
}
