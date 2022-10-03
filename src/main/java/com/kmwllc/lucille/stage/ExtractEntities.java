package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.util.FileUtils;
import com.kmwllc.lucille.util.StageUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import java.io.File;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import org.ahocorasick.trie.PayloadEmit;
import org.ahocorasick.trie.PayloadToken;
import org.ahocorasick.trie.PayloadTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Stage for performing dictionary based entity extraction on a given field, using terms from a
 * given dictionary file. The dictionary file should have a term on each line, and can support
 * providing payloads with the syntax "term, payload". If any occurrences are found, they will be
 * extracted and their associated payloads will be appended to the destination field.
 *
 * <p>Config Parameters:
 *
 * <p>- source (List<String>) : list of source field names - dest (List<String>) : list of
 * destination field names. You can either supply the same number of source and destination fields
 * for a 1-1 mapping of results or supply one destination field for all of the source fields to be
 * mapped into. - dict_path (String) : The path the dictionary to use for matching. If the dict_path
 * begins with "classpath:" the classpath will be searched for the file. Otherwise, the local file
 * system will be searched. - use_payloads (Boolean, Optional) : denotes whether paylaods from the
 * dictionary should be used or not. - update_mode (String, Optional) : Determines how writing will
 * be handling if the destination field is already populated. Can be 'overwrite', 'append' or
 * 'skip'. Defaults to 'overwrite'. - ignore_case (Boolean, Optional) : Denotes whether this Stage
 * will ignore case determining when making matches. Defaults to false. - only_whitespace_separated
 * (Boolean, Optional) : Denotes whether terms must be whitespace separated to be candidates for
 * matching. Defaults to false. - stop_on_hit (Boolean, Optional) : Denotes whether this matcher
 * should stop after one hit. Defaults to false. - only_whole_words (Boolean, Optional) : Determines
 * whether this matcher will trigger for matches contained within other text. ie "OMAN" in "rOMAN".
 * Defaults to false. - ignore_overlaps (Boolean, Optional) : Decides whether overlapping matches
 * should both be extracted or if only the longer, left most match should be kept. Defaults to true.
 */
public class ExtractEntities extends Stage {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PayloadTrie<String> dictTrie;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final List<String> dictionaries;
  private final UpdateMode updateMode;

  private final boolean ignoreCase;
  private final boolean onlyWhitespaceSeparated;
  private final boolean stopOnHit;
  private final boolean onlyWholeWords;
  private final boolean ignoreOverlaps;
  private final boolean usePayloads;
  private final String entityField;

  public ExtractEntities(Config config) {
    super(
        new StageSpec(config)
            .withRequiredProperties("source", "dest", "dictionaries")
            .withOptionalProperties(
                "ignore_case",
                "only_whitespace_separated",
                "stop_on_hit",
                "only_whole_words",
                "ignore_overlaps",
                "use_payloads",
                "update_mode",
                "entity_field"));

    this.sourceFields = config.getStringList("source");
    this.destFields = config.getStringList("dest");
    this.dictionaries = config.getStringList("dictionaries");

    // For the optional settings, we check if the config has this setting and then what the value
    // is.
    this.updateMode = UpdateMode.fromConfig(config);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.onlyWhitespaceSeparated =
        ConfigUtils.getOrDefault(config, "only_whitespace_separated", false);
    this.stopOnHit = ConfigUtils.getOrDefault(config, "stop_on_hit", false);
    this.onlyWholeWords = ConfigUtils.getOrDefault(config, "only_whole_words", true);
    this.ignoreOverlaps = ConfigUtils.getOrDefault(config, "ignore_overlaps", false);
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.entityField = config.hasPath("entity_field") ? config.getString("entity_field") : null;
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "Extract Entities");
    StageUtils.validateFieldNumNotZero(destFields, "Extract Entities");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "Extract Entities");

    dictTrie = buildTrie();
  }

  /**
   * Generate a Trie to perform dictionary based entity extraction with
   *
   * @return a PayloadTrie capable of finding matches for its dictionary values
   */
  private PayloadTrie<String> buildTrie() throws StageException {
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

    for (String dictFile : dictionaries) {
      File d = new File(dictFile);
      log.info("loading Dictionary from {}", d.getAbsolutePath());
      try (CSVReader reader = new CSVReader(FileUtils.getReader(dictFile))) {
        // For each line of the dictionary file, add a keyword/payload pair to the Trie
        String[] line;
        boolean ignore = false;
        while ((line = reader.readNext()) != null) {
          if (line.length == 0) continue;

          for (String term : line) {
            if (term.contains("\uFFFD")) {
              log.warn(
                  String.format(
                      "Entry \"%s\" on line %d contained malformed characters which were removed. "
                          + "This dictionary entry will be ignored.",
                      term, reader.getLinesRead()));
              ignore = true;
              break;
            }
          }

          if (ignore) {
            ignore = false;
            continue;
          }

          if (line.length == 1) {
            String word = line[0].trim();
            trieBuilder = trieBuilder.addKeyword(word, word);
          } else {
            trieBuilder = trieBuilder.addKeyword(line[0].trim(), line[1].trim());
          }
        }
      } catch (Exception e) {
        throw new StageException("Failed to read from the given file.", e);
      }
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

      if (!doc.has(sourceField)) continue;

      // Parse the matches and then convert the PayloadEmits into a List of Strings, representing
      // the payloads for
      // each match which occurred in the input string.
      Collection<PayloadEmit<String>> results = new ArrayList<>();
      Collection<PayloadToken<String>> tokens = new ArrayList<>();
      for (String value : doc.getStringList(sourceField)) {
        results.addAll(dictTrie.parseText(value));
        tokens.addAll(dictTrie.tokenize(value));
      }

      List<String> payloads;
      if (usePayloads) {
        payloads = results.stream().map(PayloadEmit::getPayload).collect(Collectors.toList());
      } else {
        payloads = results.stream().map(PayloadEmit::getKeyword).collect(Collectors.toList());
      }
      if (payloads.isEmpty()) continue;

      doc.update(destField, updateMode, payloads.toArray(new String[0]));

      if (entityField != null && usePayloads) {
        payloads = results.stream().map(PayloadEmit::getKeyword).collect(Collectors.toList());
        if (payloads.isEmpty()) {
          continue;
        }
        doc.update(entityField, updateMode, payloads.toArray(new String[0]));
      }
    }

    return null;
  }
}
