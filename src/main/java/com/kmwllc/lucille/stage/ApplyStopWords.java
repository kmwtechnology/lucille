package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.ahocorasick.trie.PayloadToken;
import org.ahocorasick.trie.PayloadTrie;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This Stage removes stop words supplied in the dictionaries from a given list of fields. Multi term stop "phrases"
 * are allowed and will be completely removed from the output String. NOTE : This Stage will only remove whole words and
 * will make case insensitive matches.
 *
 * Config Parameters -
 *
 *   - dictionaries (List<String>) : A list of dictionaries to pull stop words from
 *   - fields (List<String>, Optional) : A list of fields to remove stop words from. If no fields are supplied, this
 *     stage will be applied to every non-reserved field on every document.
 */
public class ApplyStopWords extends Stage {

  private final List<String> dictionaries;
  // private final List<String> stopWords;
  private final List<String> fieldNames;

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private PayloadTrie<String> dictTrie;

  public ApplyStopWords(Config config) {
    super(config);
    this.dictionaries = config.getStringList("dictionaries");
    this.fieldNames = config.hasPath("fields") ? config.getStringList("fields") : null;
  }

  @Override
  public void start() throws StageException {
    if (dictionaries.isEmpty())
      throw new StageException("Must provide at least one dictionary containing stop words to the ApplyStopWords Stage.");
    dictTrie = buildTrie(dictionaries);
  }

  /**
   * Generate a Trie to perform dictionary based entity extraction with
   *
   * @param dictionaries  a list of dictionary files to read from
   * @return  a PayloadTrie capable of finding matches for its dictionary values
   */
  private PayloadTrie<String> buildTrie(List<String> dictionaries) throws StageException {
    PayloadTrie.PayloadTrieBuilder<String> trieBuilder = PayloadTrie.builder();
    trieBuilder = trieBuilder.ignoreCase();
    trieBuilder = trieBuilder.onlyWholeWordsWhiteSpaceSeparated();
    trieBuilder = trieBuilder.ignoreOverlaps();

    for (String dictFile : dictionaries) {

      try (CSVReader reader = new CSVReader(FileUtils.getReader(dictFile))) {
        // For each line of the dictionary file, add a keyword/payload pair to the Trie
        String[] line;
        boolean ignore = false;
        while ((line = reader.readNext()) != null) {
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

          String word = line[0].trim();
          trieBuilder = trieBuilder.addKeyword(word, word);
        }
      } catch (Exception e) {
        throw new StageException("Failed to read from the given file.", e);
      }
    }

    return trieBuilder.build();
  }

  @Override
  public List<Document> processDocument(Document doc) throws StageException {
    if (fieldNames == null || fieldNames.size() == 0 ) {
      log.warn("No field names to remove stop words from supplied.");
    }

    List<String> fields = fieldNames != null ? fieldNames : new ArrayList<>(doc.getFieldNames());
    fields.removeAll(Document.RESERVED_FIELDS);

    for (String field : fields) {
      if (!doc.has(field))
        continue;

      // TODO : Decide how we want to handle punctuation (currently it is left in, see tests for examples)
      List<String> newValues = new ArrayList<>();
      for (String val : doc.getStringList(field)) {
        if (val == null)
          continue;

        if (dictTrie.containsMatch(val)) {
          Stream<PayloadToken<String>> tokenStream = dictTrie.tokenize(val).stream();
          tokenStream = tokenStream.filter((PayloadToken<String> token) -> token.getEmit() == null && !token.getFragment().isBlank());
          newValues.add(tokenStream.map((PayloadToken<String> token) -> token.getFragment().trim()).collect(Collectors.joining(" ")));
        } else {
          newValues.add(val.trim());
        }
      }

      doc.update(field, UpdateMode.OVERWRITE, newValues.toArray(new String[0]));
    }

    return null;
  }
}
