package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.util.StageUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Iterator;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.FSTCompiler;
import org.apache.lucene.util.fst.NoOutputs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Extracts dictionary-based entities from document text fields using a Lucene FST. Matches can optionally map to
 * payload values, control overlap behavior, and quit on first hit. Dictionaries are read as UTF-8 meaning all files
 * must be UTF-8 encoded. Any invalid bytes will prevent those entries from matching during extraction.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>dictionaries (List&lt;String&gt;, Required) : Paths to CSV dictionary files. Each row’s first column is the extraction term
 *   and the second column (optional) is the payload.</li>
 *   <li>source (List&lt;String&gt;, Required) : List of source field names.</li>
 *   <li>dest (List&lt;String&gt;, Required) : List of destination field names. You can either supply the same number of source and destination
 *   fields for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.</li>
 *   <li>use_payloads (Boolean, Optional) : Denotes whether payloads from the dictionary should be used or not.</li>
 *   <li>only_whole_words (Boolean, Optional) : If true, matches must be bounded by non-letters on both sides (word boundaries). If
 *   false, substrings inside larger words are allowed. Defaults to true.</li>
 *   <li>ignore_overlaps (Boolean, Optional) : If true, emits only the single longest match starting at a position; if false, emits
 *   all overlapping matches that start at that position. Defaults to false.</li>
 *   <li>stop_on_hit (Boolean, Optional) : Denotes whether this matcher should stop after one hit. Defaults to false.</li>
 *   <li>ignore_case (Boolean, Optional) : Denotes whether this Stage will ignore case determining when making matches. Defaults to false.</li>
 *   <li>update_mode (String, Optional) : Determines how writing will be handled if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 *   <li>dicts_sorted (Boolean, Optional) : If true, assumes dictionary rows are already lexicographically sorted by the key (trimmed and lowercased
 *   if ignore_case=true). Skips sorting to reduce startup time. Defaults to false.</li>
 *   <li>entity_field (String, Optional) : When set and use_payloads=true, also writes the matched normalized surface terms to this field.</li>
 *   <li>s3 (Map, Optional) : If your dictionary files are held in S3. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>azure (Map, Optional) : If your dictionary files are held in Azure. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>gcp (Map, Optional) : If your dictionary files are held in Google Cloud. See FileConnector for the appropriate arguments to provide.</li>
 * </ul>
 */
public class ExtractEntitiesFST extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredList("dictionaries", new TypeReference<List<String>>() {})
      .requiredList("source", new TypeReference<List<String>>() {})
      .requiredList("dest", new TypeReference<List<String>>() {})
      .optionalBoolean("use_payloads", "only_whole_words", "ignore_overlaps", "stop_on_hit", "ignore_case", "dicts_sorted")
      .optionalString("update_mode", "entity_field")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .build();

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  // Config
  private final List<String> dictionaries;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final boolean usePayloads;
  private final boolean onlyWholeWords;
  private final boolean ignoreOverlaps;
  private final boolean stopOnHit;
  private final boolean ignoreCase;
  private final boolean dictsSorted;
  private final String entityField;
  private final UpdateMode updateMode;

  // FSTs
  private FST<Object> fstNoPayloads; // used when use_payloads=false
  private FST<BytesRef> fstPayloads; // used when use_payloads=true

  public ExtractEntitiesFST(Config config) throws StageException {
    super(config);
    this.dictionaries = config.getStringList("dictionaries"); // Dict files to load
    this.sourceFields = config.getStringList("source"); // Fields in the input doc to read from
    this.destFields = config.getStringList("dest"); // Fields in the doc to write matched results to
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.onlyWholeWords = ConfigUtils.getOrDefault(config, "only_whole_words", true);
    this.ignoreOverlaps = ConfigUtils.getOrDefault(config, "ignore_overlaps", false);
    this.stopOnHit = ConfigUtils.getOrDefault(config, "stop_on_hit", false);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.dictsSorted = ConfigUtils.getOrDefault(config, "dicts_sorted", false);
    this.entityField = ConfigUtils.getOrDefault(config, "entity_field", null);
    this.updateMode = UpdateMode.fromConfig(config);
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "ExtractEntitiesFST");
    StageUtils.validateFieldNumNotZero(destFields, "ExtractEntitiesFST");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "ExtractEntitiesFST");

    try {
      if (usePayloads) {
        buildFSTWithPayloads();
      } else {
        buildFSTNoPayloads();
      }
    } catch (IOException e) {
      throw new StageException("Failed to initialize dictionaries for ExtractEntitiesFST", e);
    }
  }

  // Load and normalize terms with payloads (trim + optional lowercase)
  private LinkedHashMap<String, String> loadTermsAndPayloads() throws StageException {
    LinkedHashMap<String, String> termToPayload = new LinkedHashMap<>();

    for (String dictFile : dictionaries) {
      log.info("loading Dictionary from {}", dictFile);
      try (CSVReader reader = new CSVReader(FileContentFetcher.getOneTimeReader(dictFile, config))) {
        String[] line;
        while (true) {
          try {
            line = reader.readNext();
          } catch (Exception e) {
            throw new StageException("Error reading dictionary file: " + dictFile, e);
          }

          // End of file
          if (line == null) {
            break;
          }

          // Skip empty rows
          if (line.length == 0 || line[0] == null) {
            continue;
          }

          boolean ignore = false;
          for (String cell : line) {
            if (cell != null && cell.contains("\uFFFD")) {
              log.warn("Dictionary entry contained malformed characters and will be ignored. FILE={}, LINE={}", dictFile, reader.getLinesRead());
              ignore = true;
              break;
            }
          }

          if (ignore) {
            continue;
          }

          // Normalize term with trim and lowercase
          String term = normalizeDictKey(line[0]);
          if (term.isEmpty() || termToPayload.containsKey(term)) {
            continue;
          }

          // Get the payload string
          String payload;
          if (line.length > 1) {
            String p = line[1];
            payload = (p == null) ? "" : p.trim();
          } else {
            payload = term;
          }

          termToPayload.put(term, payload);
        }
      } catch (Exception e) {
        throw new StageException("Error reading dictionary file: " + dictFile, e);
      }
    }

    return termToPayload;
  }

  private void buildFSTNoPayloads() throws IOException, StageException {
    // Sort terms lexicographically
    List<String> terms = new ArrayList<>(loadTermsAndPayloads().keySet());

    if (!dictsSorted) {
      terms.sort(Comparator.naturalOrder());
    }

    var outputs = NoOutputs.getSingleton();
    FSTCompiler<Object> builder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE2, outputs).build();

    // Add each unique sorted term into the FST
    String last = null;
    for (String t : terms) {
      // Since terms are sorted duplicates will always be sequential
      if (last != null && t.equals(last)) {
        continue;
      }
      last = t;
      builder.add(toIntsRef(t), outputs.getNoOutput());
    }

    // Compile and load the FST
    FST.FSTMetadata<Object> meta = builder.compile();
    fstNoPayloads = FST.fromFSTReader(meta, builder.getFSTReader());
  }

  private void buildFSTWithPayloads() throws IOException, StageException {
    // Convert map entries into a list and sort lexicographically
    List<Map.Entry<String, String>> rows = new ArrayList<>(loadTermsAndPayloads().entrySet());

    if (!dictsSorted) {
      rows.sort(Map.Entry.comparingByKey());
    }

    var outputs = ByteSequenceOutputs.getSingleton();
    FSTCompiler<BytesRef> builder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE2, outputs).build();

    // Add each unique sorted term and payload pair into the FST
    String last = null;
    for (Map.Entry<String, String> e : rows) {
      String t = e.getKey();
      // Since terms are sorted duplicates will always be sequential
      if (last != null && t.equals(last)) {
        continue;
      }

      last = t;
      String payload = e.getValue();
      if (payload == null) {
        payload = t;
      }

      builder.add(toIntsRef(t), new BytesRef(payload));
    }

    // Compile and load the FST
    FST.FSTMetadata<BytesRef> meta = builder.compile();
    fstPayloads = FST.fromFSTReader(meta, builder.getFSTReader());
  }

  private static final class MatchHit {
    final String key;     // normalized key
    final String payload; // payload or null if use_payloads=false
    final int length;     // length in characters
    MatchHit(String key, String payload, int length) {
      this.key = key;
      this.payload = payload;
      this.length = length;
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // Iterate over each configured source field
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

      // Skip if the doc is missing the source field
      if (!doc.has(sourceField)) {
        continue;
      }

      // Read all string values from the source field
      List<String> sourceValues = doc.getStringList(sourceField);
      if (sourceValues == null || sourceValues.isEmpty()) {
        continue;
      }

      // Collect results for this field
      List<String> outputs = new ArrayList<>();
      List<String> matchedTerms = entityField != null && usePayloads ? new ArrayList<>() : null;

      // Scan each string value in the source field
      for (String raw : sourceValues) {
        if (raw == null || raw.isEmpty()) {
          continue;
        }

        findMatches(raw, outputs, matchedTerms);

        if (stopOnHit && !outputs.isEmpty()) {
          break;
        }
      }

      // If we found any matches, write them to the document
      if (!outputs.isEmpty()) {
        doc.update(destField, updateMode, outputs.toArray(new String[0]));
        if (matchedTerms != null && !matchedTerms.isEmpty()) {
          doc.update(entityField, updateMode, matchedTerms.toArray(new String[0]));
        }
      }
    }

    return null;
  }

  // --- Character-based matching ---

  private static boolean isLeftBoundary(String s, int start) {
    if (start <= 0) {
      return true;
    }
    
    int cp = s.codePointBefore(start);
    return !Character.isLetter(cp);
  }

  private static boolean isRightBoundary(String s, int end) {
    if (end >= s.length()) {
      return true;
    }
    
    int cp = s.codePointAt(end);
    return !Character.isLetter(cp);
  }

  private void findMatches(String raw, List<String> outputs, List<String> matchedTerms) {
    String text = ignoreCase ? raw.toLowerCase(Locale.ROOT) : raw;
    int n = text.length();

    if (usePayloads) {
      FST<BytesRef> fst = fstPayloads;
      for (int i = 0; i < n; ) {
        FST.BytesReader r = fst.getBytesReader();
        FST.Arc<BytesRef> arc = fst.getFirstArc(new FST.Arc<>());
        BytesRef out = fst.outputs.getNoOutput();

        MatchHit longest = null;
        List<MatchHit> allAtI = ignoreOverlaps ? null : new ArrayList<>(2);

        int j = i;
        while (j < n) {
          int label = text.charAt(j);

          try {
            if (fst.findTargetArc(label, arc, arc, r) == null) {
              break;
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }

          out = fst.outputs.add(out, arc.output());
          int j1 = j + 1;

          if (arc.isFinal() && (!onlyWholeWords || (isLeftBoundary(raw, i) && isRightBoundary(raw, j1)))) {
            BytesRef payloadBR = fst.outputs.add(out, arc.nextFinalOutput());
            String payload = payloadBR == null ? "" : payloadBR.utf8ToString();
            String key = text.substring(i, j1);
            MatchHit hit = new MatchHit(key, payload, j1 - i);

            if (ignoreOverlaps) {
              if (longest == null || hit.length > longest.length) {
                longest = hit;
              }
            } else {
              allAtI.add(hit);
            }
          }

          j = j1;
        }

        if (ignoreOverlaps) {
          if (longest != null) {
            outputs.add(longest.payload);

            if (matchedTerms != null) {
              matchedTerms.add(longest.key);
            }
            i += Math.max(1, longest.length);
          } else {
            i++;
          }
        } else {
          if (allAtI != null && !allAtI.isEmpty()) {
            for (MatchHit m : allAtI) {
              outputs.add(m.payload);
              if (matchedTerms != null) {
                matchedTerms.add(m.key);
              }
            }
          }
          i++;
        }

        if (stopOnHit && !outputs.isEmpty()) {
          break;
        }
      }
    } else {
      FST<Object> fst = fstNoPayloads;

      for (int i = 0; i < n; ) {
        FST.BytesReader r = fst.getBytesReader();
        FST.Arc<Object> arc = fst.getFirstArc(new FST.Arc<>());

        MatchHit longest = null;
        List<MatchHit> allAtI = ignoreOverlaps ? null : new ArrayList<>(2);

        int j = i;
        while (j < n) {
          int label = text.charAt(j);
          try {
            if (fst.findTargetArc(label, arc, arc, r) == null) {
              break;
            }
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          int j1 = j + 1;

          if (arc.isFinal() && (!onlyWholeWords || (isLeftBoundary(raw, i) && isRightBoundary(raw, j1)))) {
            String key = text.substring(i, j1);
            MatchHit hit = new MatchHit(key, null, j1 - i);

            if (ignoreOverlaps) {
              if (longest == null || hit.length > longest.length) {
                longest = hit;
              }
            } else {
              allAtI.add(hit);
            }
          }

          j = j1;
        }

        if (ignoreOverlaps) {
          if (longest != null) {
            outputs.add(longest.key);
            i += Math.max(1, longest.length);
          } else {
            i++;
          }
        } else {
          if (allAtI != null && !allAtI.isEmpty()) {
            for (MatchHit m : allAtI) {
              outputs.add(m.key);
            }
          }

          i++;
        }

        if (stopOnHit && !outputs.isEmpty()) {
          break;
        }
      }
    }
  }

  private static IntsRef toIntsRef(String s) {
    IntsRefBuilder irb = new IntsRefBuilder();
    irb.grow(s.length());
    irb.clear();

    for (int i = 0; i < s.length(); i++) {
      irb.append(s.charAt(i));
    }

    return irb.get();
  }

  private String normalizeDictKey(String s) {
    if (s == null) {
      return "";
    }

    String key = s.trim();

    if (ignoreCase) {
      key = key.toLowerCase(Locale.ROOT);
    }

    return key;
  }
}