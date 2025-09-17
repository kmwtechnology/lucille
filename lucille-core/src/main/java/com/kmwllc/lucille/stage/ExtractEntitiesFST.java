package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.util.StageUtils;
import com.opencsv.CSVReader;
import com.typesafe.config.Config;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.*;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * Extracts dictionary-based entities from document text fields using a Lucene FST. Matches can optionally map to
 * payload values, control overlap behavior, and quit on first hit.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>dictionaries (List&lt;String&gt;, Required) : Paths to CSV dictionary files. Each row’s first column is the extraction term
 *   and the second column (optional) is the payload.</li>
 *   <li>source (List&lt;String&gt;, Required) : List of source field names.</li>
 *   <li>dest (List&lt;String&gt;, Required) : List of destination field names. You can either supply the same number of source and destination
 *   fields for a 1-1 mapping of results or supply one destination field for all of the source fields to be mapped into.</li>
 *   <li>use_payloads (Boolean, Optional) : Denotes whether payloads from the dictionary should be used or not.</li>
 *   <li>ignore_overlaps (Boolean, Optional) : If true, emits only the single longest match starting at a position; if false, emits
 *   all overlapping matches that start at that position. Defaults to false.</li>
 *   <li>stop_on_hit (Boolean, Optional) : Denotes whether this matcher should stop after one hit. Defaults to false.</li>
 *   <li>ignore_case (Boolean, Optional) : Denotes whether this Stage will ignore case determining when making matches. Defaults to false.</li>
 *   <li>update_mode (String, Optional) : Determines how writing will be handled if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
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
      .optionalBoolean("use_payloads", "ignore_overlaps", "stop_on_hit", "ignore_case")
      .optionalString("update_mode", "entity_field")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .build();

  // Config
  private final List<String> dictionaries;
  private final List<String> sourceFields;
  private final List<String> destFields;
  private final boolean usePayloads;
  private final boolean ignoreOverlaps;
  private final boolean stopOnHit;
  private final boolean ignoreCase;
  private final String entityField;
  private final UpdateMode updateMode;

  // IO
  private final FileContentFetcher fileFetcher;

  // Analysis
  private final Analyzer analyzer;

  // FSTs
  private FST<Object> fstNoPayloads;   // used when use_payloads=false
  private FST<BytesRef> fstPayloads; // used when use_payloads=true

  public ExtractEntitiesFST(Config config) {
    super(config);
    this.dictionaries = config.getStringList("dictionaries"); // Dict files to load
    this.sourceFields = config.getStringList("source"); // Fields in the input doc to read from
    this.destFields = config.getStringList("dest"); // Fields in the doc to write matched results to
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.ignoreOverlaps = ConfigUtils.getOrDefault(config, "ignore_overlaps", false);
    this.stopOnHit = ConfigUtils.getOrDefault(config, "stop_on_hit", false);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.entityField = ConfigUtils.getOrDefault(config, "entity_field", null);
    this.updateMode = UpdateMode.fromConfig(config);
    this.fileFetcher = new FileContentFetcher(config);

    if (ignoreCase) {
      this.analyzer = new StandardAnalyzer();
    } else {
      this.analyzer = new Analyzer() {
        @Override
        protected TokenStreamComponents createComponents(String fieldName) { return new TokenStreamComponents(new StandardTokenizer()); }
      };
    }
  }

  @Override
  public void start() throws StageException {
    StageUtils.validateFieldNumNotZero(sourceFields, "ExtractEntitiesFST");
    StageUtils.validateFieldNumNotZero(destFields, "ExtractEntitiesFST");
    StageUtils.validateFieldNumsSeveralToOne(sourceFields, destFields, "ExtractEntitiesFST");

    try {
      fileFetcher.startup();
      if (usePayloads) {
        buildFSTWithPayloads();
      } else {
        buildFSTNoPayloads();
      }
    } catch (IOException e) {
      throw new StageException("Failed to initialize dictionaries for ExtractEntitiesFST", e);
    }
  }

  @Override
  public void stop() throws StageException {
    fileFetcher.shutdown();
  }

  // Load and normalize terms with payloads
  private LinkedHashMap<String, String> loadTermsAndPayloads() throws IOException, StageException {
    LinkedHashMap<String, String> termToPayload = new LinkedHashMap<>();

    // Read each dict in our list
    for (String dictFile : dictionaries) {
      try (CSVReader reader = new CSVReader(fileFetcher.getReader(dictFile))) {
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

          // Normalize term with trim and lowercase
          String term = normalizeKey(line[0].trim());
          if (term.isEmpty() || termToPayload.containsKey(term)) {
            continue;
          }

          // Get the payload string
          String payload = (line.length > 1 && line[1] != null && !line[1].trim().isEmpty())
              ? line[1].trim()
              : null;

          termToPayload.put(term, payload);
        }
      }
    }

    return termToPayload;
  }

  private void buildFSTNoPayloads() throws IOException, StageException {
    // Sort terms lexicographically
    List<String> terms = new ArrayList<>(loadTermsAndPayloads().keySet());
    terms.sort(Comparator.naturalOrder());

    // Initialize FST builder with no payloads
    var rw = FSTCompiler.getOnHeapReaderWriter(15);
    var outputs = NoOutputs.getSingleton();
    FSTCompiler<Object> builder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs)
        .dataOutput(rw)
        .build();

    // Add each unique sorted term into the FST
    String last = null;
    for (String t : terms) {
      // Since terms are sorted duplicates will always be sequential
      if (last != null && t.equals(last)) {
        continue;
      }
      last = t;
      builder.add(toIntsRef(new BytesRef(t)), outputs.getNoOutput());
    }

    // Compile and load the FST
    FST.FSTMetadata<Object> meta = builder.compile();
    fstNoPayloads = FST.fromFSTReader(meta, builder.getFSTReader());
  }

  private void buildFSTWithPayloads() throws IOException, StageException {
    // Convert map entries into a list and sort lexicographically
    List<Map.Entry<String, String>> rows = new ArrayList<>(loadTermsAndPayloads().entrySet());
    rows.sort(Map.Entry.comparingByKey());

    // Initialize FST builder with ByteSequenceOutputs
    var rw = FSTCompiler.getOnHeapReaderWriter(15);
    var outputs = ByteSequenceOutputs.getSingleton();
    FSTCompiler<BytesRef> builder = new FSTCompiler.Builder<>(FST.INPUT_TYPE.BYTE1, outputs)
        .dataOutput(rw)
        .build();

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
      if (payload == null || payload.isEmpty()) payload = t;
      builder.add(toIntsRef(new BytesRef(t)), new BytesRef(payload));
    }

    // Compile and load the FST
    FST.FSTMetadata<BytesRef> meta = builder.compile();
    fstPayloads = FST.fromFSTReader(meta, builder.getFSTReader());
  }

  private static final class MatchHit {
    final String key; // normalized "token1 token2"
    final String payload;
    final int length; // number of tokens in the match
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

        // Tokenize once
        List<String> tokens = tokenize(raw);

        int idx = 0;
        while (idx < tokens.size()) {
          int mark = idx;

          // Build the growing phrase with a StringBuilder
          StringBuilder sb = new StringBuilder();

          // Collect full matches that start at idx
          List<MatchHit> matchesHere = new ArrayList<>(2);

          boolean hasPrefix = true;
          while (hasPrefix && mark < tokens.size()) {
            if (sb.length() > 0) {
              sb.append(' ');
            }
            String tok = tokens.get(mark++);
            sb.append(tok);

            // Normalize once and reuse
            String key = normalizeKey(sb.toString());

            // Check complete match once, store payload if applicable
            if (usePayloads) {
              BytesRef br;
              try {
                br = Util.get(fstPayloads, new BytesRef(key));
              } catch (Exception e) {
                throw new RuntimeException("FST lookup failed", e);
              }
              if (br != null) {
                String payload = br.utf8ToString();
                if (payload != null && !payload.isEmpty()) {
                  matchesHere.add(new MatchHit(key, payload, (mark - idx)));
                }
              }
            } else {
              if (matchesCompletely(fstNoPayloads, new BytesRef(key))) {
                matchesHere.add(new MatchHit(key, null, (mark - idx)));
              }
            }

            // Continue only while we still have a dictionary prefix
            hasPrefix = (usePayloads)
                ? hasPrefixBytes(fstPayloads, new BytesRef(key))
                : hasPrefixBytes(fstNoPayloads, new BytesRef(key));
          }

          // Emit according to overlap rules using precomputed results
          MatchHit chosen = null;
          if (!matchesHere.isEmpty()) {
            if (!ignoreOverlaps) {
              for (MatchHit m : matchesHere) {
                if (usePayloads) {
                  outputs.add(m.payload);
                  if (matchedTerms != null) {
                    matchedTerms.add(m.key);
                  }
                } else {
                  outputs.add(m.key);
                }
              }
            } else {
              for (MatchHit m : matchesHere) {
                if (chosen == null || m.length > chosen.length) {
                  chosen = m;
                }
              }
              if (chosen != null) {
                if (usePayloads) {
                  outputs.add(chosen.payload);
                  if (matchedTerms != null) {
                    matchedTerms.add(chosen.key);
                  }
                } else {
                  outputs.add(chosen.key);
                }
              }
            }
          }

          // Advance
          if (ignoreOverlaps && chosen != null) {
            idx += Math.max(1, chosen.length);
          } else {
            idx++;
          }

          // Early exit
          if (stopOnHit && !outputs.isEmpty()) {
            break;
          }
        }

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

  // Tokenize a provided string
  private List<String> tokenize(String input) {
    // Used to hold extracted tokens
    List<String> out = new ArrayList<>();

    // Create a TokenStream using the input string
    try (TokenStream ts = analyzer.tokenStream(null, new StringReader(input))) {
      ts.reset();
      CharTermAttribute term = ts.getAttribute(CharTermAttribute.class);
      // Iterate through all tokens in the stream
      while (ts.incrementToken()) {
        out.add(term.toString());
      }
      ts.end();
    } catch (IOException e) {
      // StringReader -> shouldn't happen
      throw new RuntimeException(e);
    }

    return out;
  }

  // Check if a sequence of tokens forms a complete dictionary match
  private boolean isCompleteMatch(List<String> tokens) {
    // Join tokens in to normalized lowercase string
    String key = normalizeKey(String.join(" ", tokens));
    if (usePayloads) {
      try {
        // Look up exact key in FST with outputs to get payload if found
        return Util.get(fstPayloads, new BytesRef(key)) != null;
      } catch (Exception e) {
        throw new RuntimeException("FST lookup failed", e);
      }
    } else {
      // Run manual check since no output will be returned
      return matchesCompletely(fstNoPayloads, new BytesRef(key));
    }
  }

  // Verify that input matches a complete entry
  private static boolean matchesCompletely(FST<Object> fst, BytesRef input) {
    try {
      // Get a reader to traverse FST
      FST.BytesReader r = fst.getBytesReader();
      // Start from the root
      FST.Arc<Object> arc = fst.getFirstArc(new FST.Arc<>());

      // Walk through each byte in the input
      for (int i = 0; i < input.length; i++) {
        int label = input.bytes[input.offset + i] & 0xFF;
        // If there is no arc for this byte it's not a match
        if (fst.findTargetArc(label, arc, arc, r) == null) {
          return false;
        }
      }

      // Must end on a final arc
      return arc.isFinal();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Test if the input has a valid prefix of any term
  private static <T> boolean hasPrefixBytes(FST<T> fst, BytesRef input) {
    try {
      // Reader over the compiled FST bytes
      FST.BytesReader r = fst.getBytesReader();
      // Start at the root
      FST.Arc<T> arc = fst.getFirstArc(new FST.Arc<>());

      // Walk byte by byte through the input string
      for (int i = 0; i < input.length; i++) {
        int label = input.bytes[input.offset + i] & 0xFF;

        // If there's no valid transition for this byte then it's not a prefix
        if (fst.findTargetArc(label, arc, arc, r) == null) {
          return false;
        }
      }
      return true;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static IntsRef toIntsRef(BytesRef b) {
    IntsRefBuilder irb = new IntsRefBuilder();
    irb.grow(b.length);
    irb.clear();
    for (int i = 0; i < b.length; i++) {
      irb.append(b.bytes[b.offset + i] & 0xFF);
    }
    return irb.get();
  }

  // Normalize a key based on the ignore case option
  private String normalizeKey(String s) {
    String t = (s == null) ? "" : s.trim();
    return ignoreCase ? t.toLowerCase(Locale.ROOT) : t;
  }
}