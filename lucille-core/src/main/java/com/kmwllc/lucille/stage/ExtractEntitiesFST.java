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
import com.typesafe.config.ConfigFactory;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
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
 *   <li>update_mode (String, Optional) : Determines how writing will be handled if the destination field is already populated. Can be
 *   'overwrite', 'append' or 'skip'. Defaults to 'overwrite'.</li>
 *   <li>entity_field (String, Optional) : When set and use_payloads=true, also writes the matched normalized surface terms
 *   to this field.</li>
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
      .optionalBoolean("use_payloads", "ignore_overlaps", "stop_on_hit")
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
  private final String entityField;
  private final UpdateMode updateMode;

  // IO
  private final FileContentFetcher fileFetcher;

  // Analysis
  private final Analyzer analyzer = new StandardAnalyzer();

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
    this.entityField = ConfigUtils.getOrDefault(config, "entity_field", null);
    this.updateMode = UpdateMode.fromConfig(config);
    this.fileFetcher = new FileContentFetcher(config);
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

  private void buildFSTNoPayloads() throws IOException, StageException {
    // Used to de-duplicate terms across all dictionaries
    HashSet<String> termsSet = new HashSet<>();

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
          if (line.length == 0) {
            continue;
          }

          // Get only the term column
          String raw = line[0];
          if (raw == null) {
            continue;
          }

          // Normalize term with trim and lowercase
          final String term = raw.trim().toLowerCase(Locale.ROOT);
          if (!term.isEmpty()) {
            termsSet.add(term);
          }
        }
      }
    }

    // Sort terms lexicographically
    List<String> terms = new ArrayList<>(termsSet);
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
    // Used to store term -> payload
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
          final String term = line[0].trim().toLowerCase(Locale.ROOT);
          if (term.isEmpty() || termToPayload.containsKey(term)) {
            continue;
          }

          // Get the payload string
          String payloadKey = (line.length > 1 && line[1] != null && !line[1].trim().isEmpty())
              ? line[1].trim()
              : term;

          termToPayload.put(term, payloadKey);
        }
      }
    }

    // Convert map entries into a list and sort lexicographically
    List<Map.Entry<String, String>> rows = new ArrayList<>(termToPayload.entrySet());
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
      builder.add(toIntsRef(new BytesRef(t)), new BytesRef(payload));
    }

    // Compile and load the FST
    FST.FSTMetadata<BytesRef> meta = builder.compile();
    fstPayloads = FST.fromFSTReader(meta, builder.getFSTReader());
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // Iterate over each configured source field
    for (int i = 0; i < sourceFields.size(); i++) {
      String sourceField = sourceFields.get(i);
      String destField   = destFields.size() == 1 ? destFields.get(0) : destFields.get(i);

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

        // Tokenize the text with the Analyzer
        List<String> tokens = tokenize(raw);

        int idx = 0;
        // Walk over tokens, growing the phrases as long as the prefixes exists in the FST
        while (idx < tokens.size()) {
          int mark = idx;
          // The current growing phrase
          List<String> cur = new ArrayList<>();
          // All full matches starting at the index
          List<List<String>> matchesHere = new ArrayList<>();

          // Grow the phrase while it remains a prefix of at least one dictionary entry
          do {
            cur.add(tokens.get(mark++));
            if (isCompleteMatch(cur)) {
              matchesHere.add(new ArrayList<>(cur));
            }
          } while (hasPrefix(cur) && mark < tokens.size());

          // Decide what to emit from matches found at the start position
          List<String> chosen = null;
          if (matchesHere.isEmpty()) {
            // Nothing matched
          } else if (!ignoreOverlaps) {
            // Emit all matches that start here
            for (List<String> m : matchesHere) {
              appendMatch(m, outputs, matchedTerms);
            }
          } else {
            // Pick the single longest match that starts here
            for (List<String> m : matchesHere) {
              if (chosen == null || m.size() > chosen.size()) chosen = m;
            }
            appendMatch(chosen, outputs, matchedTerms);
          }

          // Advance the left index for the next attempt
          if (ignoreOverlaps && chosen != null) {
            idx += Math.max(1, chosen.size());
          } else {
            idx++;
          }

          // Exit early if we have matches
          if (stopOnHit && !outputs.isEmpty()) break;
        }

        // Exit early if we have matches
        if (stopOnHit && !outputs.isEmpty()) break;
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

  // Append a matched token sequence into the outputs list
  private void appendMatch(List<String> tokens, List<String> out, List<String> matchedTerms) {
    // Join tokens in to normalized lowercase string
    String key = String.join(" ", tokens).toLowerCase(Locale.ROOT);

    if (usePayloads) {
      BytesRef br;
      try {
        br = Util.get(fstPayloads, new BytesRef(key));
      } catch (Exception e) {
        throw new RuntimeException("FST lookup failed", e);
      }

      // If a mapping exists for the key
      if (br != null) {
        String payload = br.utf8ToString();

        if (payload == null || payload.isEmpty()) {
          // Skip if payload is empty or null
        } else {
          out.add(payload);
        }
        if (matchedTerms != null) matchedTerms.add(key);
      }
    } else {
      // Add the surface term itself if it's a complete match with no payloads
      if (isCompleteMatch(tokens)) {
        out.add(key);
      }
    }
  }

  // Check if a sequence of tokens forms a complete dictionary match
  private boolean isCompleteMatch(List<String> tokens) {
    // Join tokens in to normalized lowercase string
    String key = String.join(" ", tokens).toLowerCase(Locale.ROOT);
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

  // Wrapper that joins tokens into a normalized string and checks for any dictionary prefix
  private boolean hasPrefix(List<String> tokens) {
    // Join tokens to a single lowercase key
    String key = String.join(" ", tokens).toLowerCase(Locale.ROOT);

    return (usePayloads)
        ? hasPrefixBytes(fstPayloads, new BytesRef(key))
        : hasPrefixBytes(fstNoPayloads, new BytesRef(key));
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

  private static final List<String> DICTS = List.of(
      "/Users/henry/Desktop/generated/duns_company_to_duns_number_remapped.csv",
      "/Users/henry/Desktop/generated/duns_company_to_duns_number_standard.csv",
      "/Users/henry/Desktop/generated/duns_number_city_to_duns_number.csv",
      "/Users/henry/Desktop/generated/duns_number_country_to_duns_number.csv",
      "/Users/henry/Desktop/generated/duns_number_state_to_duns_number.csv",
      "/Users/henry/Desktop/generated/duns_number_to_duns_company.csv",
      "/Users/henry/Desktop/generated/ultimateparentduns_to_industry_segmentation_city.csv",
      "/Users/henry/Desktop/generated/ultimateparentduns_to_industry_segmentation_country.csv",
      "/Users/henry/Desktop/generated/ultimateparentduns_to_industry_segmentation_global.csv",
      "/Users/henry/Desktop/generated/ultimateparentduns_to_industry_segmentation_state.csv"
  );

  public static void main(String[] args) throws Exception {
    Config cfg = ConfigFactory.parseMap(Map.of(
        "dictionaries", DICTS,
        "source", List.of("text"),
        "dest", List.of("entities")
    ));

    ExtractEntitiesFST stage = new ExtractEntitiesFST(cfg);
    stage.start();
    System.out.println("LOOP STARTING");
    while (true) Thread.sleep(30_000);
  }

}