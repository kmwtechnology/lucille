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
import java.io.Reader;
import java.util.regex.Pattern;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.pattern.PatternReplaceCharFilter;
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
 *   <li>ignore_overlaps (Boolean, Optional) : If true, emits only the single longest match starting at a position; if false, emits
 *   all overlapping matches that start at that position. Defaults to false.</li>
 *   <li>stop_on_hit (Boolean, Optional) : Denotes whether this matcher should stop after one hit. Defaults to false.</li>
 *   <li>ignore_case (Boolean, Optional) : Denotes whether this Stage will ignore case determining when making matches. Defaults to false.</li>
 *   <li>use_whitespace_tokenizer (Boolean, Optional) : If true, tokenization uses Lucene's WhitespaceTokenizer. If token_break_regex is provided
 *   a pattern replace filter applies first to replace matches with a single space. If false, tokenization uses Lucene's StandardTokenizer.
 *   Defaults to false.</li>
 *   <li>token_break_regex (String, Optional) : Regex for characters/sequences to replace with spaces prior to tokenization. If omitted, no
 *   replacements are applied (pure whitespace tokenization). Requires use_whitespace_tokenizer=true.</li>
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
      .optionalBoolean("use_payloads", "ignore_overlaps", "stop_on_hit", "ignore_case", "use_whitespace_tokenizer", "dicts_sorted")
      .optionalString("update_mode", "entity_field", "token_break_regex")
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
  private final boolean dictsSorted;
  private final String entityField;
  private final UpdateMode updateMode;

  // Analysis
  private final Analyzer analyzer;
  private final boolean useWhitespaceTokenizer;
  private final Pattern tokenBreakPattern;

  // FSTs
  private FST<Object> fstNoPayloads;   // used when use_payloads=false
  private FST<BytesRef> fstPayloads; // used when use_payloads=true

  public ExtractEntitiesFST(Config config) throws StageException {
    super(config);
    this.dictionaries = config.getStringList("dictionaries"); // Dict files to load
    this.sourceFields = config.getStringList("source"); // Fields in the input doc to read from
    this.destFields = config.getStringList("dest"); // Fields in the doc to write matched results to
    this.usePayloads = ConfigUtils.getOrDefault(config, "use_payloads", true);
    this.ignoreOverlaps = ConfigUtils.getOrDefault(config, "ignore_overlaps", false);
    this.stopOnHit = ConfigUtils.getOrDefault(config, "stop_on_hit", false);
    this.ignoreCase = ConfigUtils.getOrDefault(config, "ignore_case", false);
    this.dictsSorted = ConfigUtils.getOrDefault(config, "dicts_sorted", false);
    this.entityField = ConfigUtils.getOrDefault(config, "entity_field", null);
    this.updateMode = UpdateMode.fromConfig(config);
    this.useWhitespaceTokenizer = ConfigUtils.getOrDefault(config, "use_whitespace_tokenizer", false);

    if (useWhitespaceTokenizer) {
      if (config.hasPath("token_break_regex")) {
        String regex = config.getString("token_break_regex");
        this.tokenBreakPattern = Pattern.compile(regex);
      } else {
        this.tokenBreakPattern = null;
      }
    } else {
      this.tokenBreakPattern = null;
    }

    if (!useWhitespaceTokenizer && config.hasPath("token_break_regex")) {
      throw new StageException("token_break_regex requires use_whitespace_tokenizer=true.");
    }

    this.analyzer = buildAnalyzer();
  }

  private Analyzer buildAnalyzer() {
    if (useWhitespaceTokenizer) {
      return new Analyzer() {
        @Override
        protected Reader initReader(String fieldName, Reader reader) {
          return (tokenBreakPattern == null)
              ? reader
              : new PatternReplaceCharFilter(tokenBreakPattern, " ", reader);
        }

        @Override
        protected TokenStreamComponents createComponents(String s) {
          WhitespaceTokenizer tokenizer = new WhitespaceTokenizer();
          TokenStream stream = tokenizer;
          if (ignoreCase) {
            stream = new LowerCaseFilter(stream);
          }
          return new TokenStreamComponents(tokenizer, stream);
        }
      };
    }

    return new Analyzer() {
      @Override
      protected TokenStreamComponents createComponents(String s) {
        // Use StandardTokenizer with custom TokenStreamComponents instead of StandardAnalyzer
        // StandardAnalyzer has a set of common stopwords (which we don't want here) and always applies lowercasing (which we want to toggle here)
        StandardTokenizer tokenizer = new StandardTokenizer();
        TokenStream stream = tokenizer;
        if (ignoreCase) {
          stream = new LowerCaseFilter(stream);
        }
        return new TokenStreamComponents(tokenizer, stream);
      }
    };
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

  // Load and normalize terms with payloads
  private LinkedHashMap<String, String> loadTermsAndPayloads() throws StageException {
    LinkedHashMap<String, String> termToPayload = new LinkedHashMap<>();

    // Read each dict in our list
    for (String dictFile : dictionaries) {
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

          // Normalize term with trim and lowercase
          String term = normalizeKey(line[0]);
          if (term.isEmpty() || termToPayload.containsKey(term)) {
            continue;
          }

          // Get the payload string
          String payload = (line.length > 1 && line[1] != null && !line[1].trim().isEmpty())
              ? line[1].trim()
              : null;

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

    // Initialize FST builder with no payloads
    // blockBits controls the block size used by the FST builder where 15 is default
    // (15 bits = 32kb) More information can be found in org.apache.lucene.util.fst.FSTCompiler.Builder
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

    if (!dictsSorted) {
      rows.sort(Map.Entry.comparingByKey());
    }

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

  private void findMatches(String raw, List<String> outputs, List<String> matchedTerms) {
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

  private static FST.Arc walk(FST fst, BytesRef input) {
    try {
      // Reader over the compiled FST bytes
      FST.BytesReader r = fst.getBytesReader();
      // Start at the root
      FST.Arc arc = fst.getFirstArc(new FST.Arc<>());

      // Walk byte by byte through the input string
      for (int i = 0; i < input.length; i++) {
        int label = input.bytes[input.offset + i] & 0xFF;

        // If there's no valid transition for this byte then it's not a prefix
        if (fst.findTargetArc(label, arc, arc, r) == null) {
          return null;
        }
      }

      return arc;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Verify that input matches a complete entry
  private static boolean matchesCompletely(FST<Object> fst, BytesRef input) {
    FST.Arc arc = walk(fst, input);
    return (arc != null) && arc.isFinal();
  }

  // Test if the input has a valid prefix of any term
  private static <T> boolean hasPrefixBytes(FST<T> fst, BytesRef input) {
    return walk(fst, input) != null;
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