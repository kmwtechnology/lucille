package com.kmwllc.lucille.stage.util;

import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.util.FileUtils;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a way for Stage instances to acquire dictionaries.
 *
 * Dictionaries are read-only and are loaded from the CSV content at a designated filesystem path.
 *
 * Dictionary instances are safe to share across threads (because they are unmodifiable HashMaps).
 *
 * DictionaryManager is intended as an optimization that eliminates the need for each Stage instance to initialize and store
 * its own dedicated dictionary instance. Using DictionaryManager, the dictionary for a given path is
 * initialized once, upon the first request to access it; the single instance of that dictionary is
 * returned upon subsequent requests.
 *
 * Stages that need to obtain a read-only dictionary from a CSV file can call
 * DictionaryManager.getDictionary("/path/to/dictionary.csv", ignoreCase)
 *
 */
public class DictionaryManager {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // a map of dictionary names to dictionary instances
  // a dictionary is named according to its CSV path plus a suffix indicating whether case is ignored
  private static final HashMap<String, Map<String, String[]>> dictionaries = new HashMap<>();

  // Private constructor to prevent instantiation.
  private DictionaryManager() {
  }

  /**
   * Returns an unmodifiable Map built from the CSV file at the designated path.
   * The Map lets us lookup a keyword and retrieve an array containing either the keyword itself
   * (indicating that the keyword is found in the given dictionary) or a sequence of payloads associated with
   * the keyword.
   *
   * If a Map has already been populated for a given path (and given setting of ignoreCase),
   * the first instance will be returned and a second instance will not be created.
   *
   * Each Stage instance that needs to acquire a dictionary should call this method once inside start().
   * Because Stage instances will not be calling this method more than once at startup time, we use a coarse-grained
   * approach of making the entire method synchronized. This is to ensure that at most one instance of each
   * named dictionary can be created. We are not concerned about the overhead of contention because
   * this method is not called repeatedly in the Stage lifecycle; it os only called once at startup.
   *
   * This implementation explicitly DOES NOT permit the concurrent loading of different dictionaries by different threads.
   * Using this implementation, only one thread can enter getDictionary() at a time, meaning that only one dictionary
   * can be initialized at a time.
   *
   * Note that the "dictionaries" HashMap is only ever accessed or updated inside this one method. Since this method
   * is synchronized, the dictionaries HashMap itself does not need to be thread-safe (by being
   * made into a ConcurrentHashMap or being passed through Collections.synchronizedMap()).
   *
   */
  public static synchronized Map<String, String[]> getDictionary(String path, boolean ignoreCase) throws StageException {
    String key = path + "_IGNORECASE=" + ignoreCase;
    if (dictionaries.containsKey(key)) {
      return dictionaries.get(key);
    }
    HashMap<String, String[]> dictionary = buildHashMap(path, ignoreCase);

    // We create an unmodifiable view of the dictionary, which is represented as a HashMap;
    // This unmodifiable Map may be shared across Stage instances running in different threads;
    // Therefore we want to be sure that concurrent read operations (i.e. map.get()'s) are thread-safe;
    // The javadoc for java.util.HashMap states:
    // "Note that this implementation is not synchronized. If multiple threads access a hash map concurrently,
    // and at least one of the threads modifies the map structurally, it must be synchronized externally."
    // Since our map is never be modified, we assume the need for synchronization does not apply.
    // Instead of returning an unmodifiable Map, we could create a ConcurrentHashMap here, but that seems to be
    // overkill given that the desired behavior is read-only.
    Map<String, String[]> unmodifiableDictionary = Collections.unmodifiableMap(dictionary);
    dictionaries.put(key, unmodifiableDictionary);
    return unmodifiableDictionary;
  }

  /**
   * Create a HashMap matching key phrases from the dictionary to payloads
   *
   * @param dictPath  the path of the dictionary file
   * @return the populated HashMap
   */
  private static HashMap<String, String[]> buildHashMap(String dictPath, boolean ignoreCase) throws StageException {
    HashMap<String, String[]> dict = new HashMap<>();
    try (CSVReader reader = new CSVReader(FileUtils.getReader(dictPath))) {
      // For each line of the dictionary file, add a keyword/payload pair
      String[] line;
      boolean ignore = false;
      while ((line = reader.readNext()) != null) {
        if (line.length == 0) {
          continue;
        }

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

        // TODO : Add log messages for when encoding errors occur so that they can be fixed
        if (line.length == 1) {
          String word = line[0].trim();
          if (ignoreCase) {
            dict.put(word.toLowerCase(), new String[]{word});
          } else {
            dict.put(word, new String[]{word});
          }
        } else {
          // Handle multiple payload values here.
          String[] rest = Arrays.copyOfRange(line, 1, line.length);
          for (int i = 0; i < rest.length; i++) {
            rest[i] = rest[i].trim();
          }
          if (ignoreCase) {
            dict.put(line[0].trim().toLowerCase(), rest);
          } else {
            dict.put(line[0].trim(), rest);
          }
        }
      }
    } catch (IOException e) {
      throw new StageException("Failed to read from the given file.", e);
    } catch (CsvValidationException e) {
      throw new StageException("Error validating CSV", e);
    }

    return dict;
  }

}
