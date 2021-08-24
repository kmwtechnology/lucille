package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;

import java.io.BufferedReader;
import java.io.FileReader;
import java.lang.invoke.MethodHandles;
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
public class EntityExtractionStage extends Stage {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private final PayloadTrie<String> dictTrie;
  private final String SOURCE_FIELDS_STR;
  private final String DEST_FIELDS_STR;
  private final Boolean IGNORE_CASE;
  private final Boolean ONLY_WHITESPACE_SEPARATED;
  private final Boolean STOP_ON_HIT;
  private final Boolean ONLY_WHOLE_WORDS;
  private final Boolean IGNORE_OVERLAPS;

  public EntityExtractionStage(Config config) {
    super(config);
    this.dictTrie = buildTrie(config.getString("dict_path"));
    this.SOURCE_FIELDS_STR = config.getString("source");
    this.DEST_FIELDS_STR = config.getString("dest");

    // For the optional settings, we check if the config has this setting and then what the value is.
    this.IGNORE_CASE = StageUtil.<Boolean>configGetOrDefault(config, "ignore_case", false);
    this.ONLY_WHITESPACE_SEPARATED = StageUtil.<Boolean>configGetOrDefault(config, "only_whitespace_separated", false);
    this.STOP_ON_HIT = StageUtil.<Boolean>configGetOrDefault(config, "stop_on_hit", false);
    this.ONLY_WHOLE_WORDS = StageUtil.<Boolean>configGetOrDefault(config, "only_whole_words", false);
    this.IGNORE_OVERLAPS = StageUtil.<Boolean>configGetOrDefault(config, "ignore_overlaps", false);
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
    if (IGNORE_CASE) {
      trieBuilder = trieBuilder.ignoreCase();
    }

    if (ONLY_WHITESPACE_SEPARATED) {
      trieBuilder = trieBuilder.onlyWholeWordsWhiteSpaceSeparated();
    }

    if (STOP_ON_HIT) {
      trieBuilder = trieBuilder.stopOnHit();
    }

    if (ONLY_WHOLE_WORDS) {
      trieBuilder = trieBuilder.onlyWholeWords();
    }

    if (IGNORE_OVERLAPS) {
      trieBuilder = trieBuilder.ignoreOverlaps();
    }

    try (BufferedReader reader = new BufferedReader(new FileReader(dictFile))) {
      // For each line of the dictionary file, add a keyword/payload pair to the Trie
      String line;
      while((line = reader.readLine()) != null) {
        String[] keyword = line.split(",");

        if (keyword.length == 1) {
          trieBuilder = trieBuilder.addKeyword(keyword[0], keyword[0]);
        } else {
          trieBuilder = trieBuilder.addKeyword(keyword[0], keyword[1]);
        }
      }
    } catch (Exception e) {
      log.error("Failed to read from the given file.", e);
    }

    return trieBuilder.build();
  }


  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    String[] srcFields = SOURCE_FIELDS_STR.split(",");
    String[] destFields = DEST_FIELDS_STR.split(",");

    StageUtil.validateFieldNumNotZero(SOURCE_FIELDS_STR, "Entity Extraction Stage");
    StageUtil.validateFieldNumNotZero(DEST_FIELDS_STR, "Entity Extraction Stage");
    StageUtil.validateFieldNumsOneToSeveral(SOURCE_FIELDS_STR, DEST_FIELDS_STR, "Entity Extraction Stage");

    int numFields = Integer.max(destFields.length, srcFields.length);

    // For each of the field names, extract dictionary values from it.
    for (int i = 0; i < numFields; i++) {
      // If there is only one source or dest, use it. Otherwise, use the current source/dest.
      String sourceField = srcFields.length == 1 ? srcFields[0] : srcFields[i];
      String destField = destFields.length == 1 ? destFields[0] : destFields[i];

      if (!doc.has(sourceField))
        continue;

      // Parse the matches and then convert the PayloadEmits into a List of Strings, representing the payloads for
      // each match which occurred in the input string.
      Collection<PayloadEmit<String>> results = dictTrie.parseText(doc.getString(sourceField));
      List<String> payloads = results.stream().map(PayloadEmit::getPayload).collect(Collectors.toList());

      if (payloads.isEmpty())
        continue;

      for (String payload : payloads) {
        doc.addToField(destField, payload);
      }
    }

    return null;
  }
}
