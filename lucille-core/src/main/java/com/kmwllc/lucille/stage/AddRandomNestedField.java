package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * Adds a nested JSON array of objects on each document from a mapping of destination paths to source fields.
 * You can set a fixed number of objects or choose a random range. If a source field is missing, optional generators
 * can supply the value. Generators or a previously defined source field can be used to supply values.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>target_field (String, Required) : Field name where the resulting JSON array of objects will be written.</li>
 *   <li>entries (Map&lt;String, String&gt;, Required) : Mapping of nested destination paths to source fields.
 *     <ul>
 *       <li>Keys are dotted paths (e.g., "user.name") that will be created inside each object.</li>
 *       <li>Values are either a source field name on the document (used if present) or the key of a configured
 *       generator (used only when the source is absent). Empty strings are invalid and will raise an error.</li>
 *       <li>Destination path segments must be non-empty (e.g., "a..b" is invalid).</li>
 *     </ul>
 *   </li>
 *   <li>num_objects (Integer, Optional) : Fixed number of nested objects to create per document. Must be a positive integer.
 *   Cannot be used together with min_num_objects/max_num_objects. Defaults to 1 when neither option is set.</li>
 *   <li>min_num_objects (Integer, Optional) : Lower bound (inclusive) on a random number of objects to create per document.
 *   Must be provided together with max_num_objects.</li>
 *   <li>max_num_objects (Integer, Optional) : Upper bound (inclusive) on a random number of objects to create per document.
 *   Must be provided together with min_num_objects.</li>
 *   <li>generators (Map, Optional) : A set of generator stage configs used to produce values when a source field in entries
 *   is missing. Each entry:
 *     <ul>
 *       <li>Requires class (fully qualified Stage implementation).</li>
 *       <li>May specify field_name; if omitted, a temporary field generator_out is written to.</li>
 *     </ul>
 *   </li>
 * </ul>
 */
public class AddRandomNestedField extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("target_field")
      .requiredParent("entries", new TypeReference<Map<String, String>>() {})
      .optionalNumber("num_objects", "min_num_objects", "max_num_objects")
      .optionalParent("generators", new TypeReference<Map<String, Object>>() {})
      .build();

  private static final String GEN_OUT_FIELD = "generator_out";

  private static final Logger log = LoggerFactory.getLogger(AddRandomNestedField.class);

  private static final ObjectMapper mapper = new ObjectMapper();

  private final String targetField;
  private final Map<List<String>, String> parsedEntries;
  private final Integer numObjects; // fixed N (optional)
  private final Integer minNumObjects; // range min (optional)
  private final Integer maxNumObjects; // range max (optional)

  private final Map<String, Stage> generators = new LinkedHashMap<>();
  private final Document genDoc;
  private final Config generatorsConfig;

  public AddRandomNestedField(Config config) throws StageException {
    super(config);
    this.targetField = ConfigUtils.getOrDefault(config, "target_field", null);
    this.numObjects = ConfigUtils.getOrDefault(config, "num_objects", null);
    this.minNumObjects = ConfigUtils.getOrDefault(config, "min_num_objects", null);
    this.maxNumObjects = ConfigUtils.getOrDefault(config, "max_num_objects", null);
    this.generatorsConfig = config.hasPath("generators") ? config.getConfig("generators") : null;
    Config entriesCfg = config.getConfig("entries");

    if (entriesCfg.root().isEmpty()) {
      throw new StageException("entries must be a non-empty mapping of 'nested.path' : 'source_field' (source_field may be empty).");
    }

    if (targetField == null || targetField.isEmpty()) {
      throw new StageException("target_field is required.");
    }

    if (Document.RESERVED_FIELDS.contains(targetField)) {
      throw new StageException("target_field '" + targetField + "' is a reserved field");
    }

    if (numObjects != null && numObjects <= 0) {
      throw new StageException("num_objects must be a positive integer if provided.");
    }

    if ((minNumObjects != null) ^ (maxNumObjects != null)) {
      throw new StageException("Both min_num_objects and max_num_objects must be provided together.");
    }

    if (minNumObjects != null) {
      if (minNumObjects <= 0 || maxNumObjects <= 0) {
        throw new StageException("min_num_objects and max_num_objects must be positive integers.");
      }
      if (minNumObjects > maxNumObjects) {
        throw new StageException("min_num_objects must be <= max_num_objects.");
      }
    }

    if (numObjects != null && (minNumObjects != null || maxNumObjects != null)) {
      throw new StageException("Specify either num_objects or (min_num_objects & max_num_objects), not both.");
    }

    this.parsedEntries = Collections.unmodifiableMap(parseEntries(entriesCfg));
    this.genDoc = Document.create("__arnf_gen_reserved__");
  }

  // Start generator stages
  @Override
  public void start() throws StageException {
    if (generatorsConfig != null) {
      // Iterate over each generator param
      for (String key : generatorsConfig.root().keySet()) {
        Config sub = generatorsConfig.getConfig(key);
        if (!sub.hasPath("class")) {
          throw new StageException("generators." + key + " must include a 'class' property");
        }

        Config injected = ConfigFactory.parseMap(Map.of(
            "name", "arnf_gen_" + key,
            "field_name", GEN_OUT_FIELD
        )).withFallback(sub);

        // Create generator
        Stage gen;

        try {
          gen = Stage.fromConfig(injected);
        } catch (Exception e) {
          throw new StageException("Failed to instantiate generator '" + key + "'", e);
        }
        gen.start();
        generators.put(key, gen);
      }
    }
  }

  @Override
  public void stop() throws StageException {
    for (Map.Entry<String, Stage> entry : generators.entrySet()) {
      String key = entry.getKey();
      Stage generator = entry.getValue();
      try {
        generator.stop();
      } catch (Exception e) {
        log.error("Error while stopping generator '{}' ({}),",
            key, generator != null ? generator.getClass().getName() : "null", e);
      }
    }
  }

  // Parse entries to split paths at .
  private static Map<List<String>, String> parseEntries(Config entriesCfg) throws StageException {
    Map<List<String>, String> out = new LinkedHashMap<>(entriesCfg.root().size());
    Map<String, Object> flat = entriesCfg.root().unwrapped();

    for (Map.Entry<String, Object> ent : flat.entrySet()) {
      String dest = ent.getKey();
      String src  = ent.getValue() == null ? "" : String.valueOf(ent.getValue());
      String trimmedDest = dest == null ? "" : dest.trim();
      String trimmedSrc = src == null ? "" : src.trim();

      if (trimmedDest.isEmpty()) {
        throw new StageException("Invalid mapping (empty destination path).");
      }

      String[] parts = trimmedDest.split("\\.");
      for (String p : parts) {
        if (p.isEmpty()) {
          throw new StageException("Invalid destination '" + trimmedDest + "' (empty segment).");
        }
      }

      List<String> key = List.of(parts);
      if (out.containsKey(key)) {
        throw new StageException("Duplicate destination '" + dest + "'.");
      }

      out.put(key, trimmedSrc);
    }

    return out;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    final int n = pickNumObjects();

    // For each object index, build destination segments
    for (int i = 0; i < n; i++) {
      final List<Document.Segment> prefix = new ArrayList<>(2);
      prefix.add(new Document.Segment(targetField));
      prefix.add(new Document.Segment(i));

      for (Map.Entry<List<String>, String> e : parsedEntries.entrySet()) {
        String sourceField = e.getValue();

        final List<Document.Segment> destFieldParts = new ArrayList<>(prefix.size() + e.getKey().size());
        destFieldParts.addAll(prefix);
        for (String s : e.getKey()) {
          if (s.chars().allMatch(c -> c >= '0' && c <= '9')) {
            destFieldParts.add(new Document.Segment(Integer.parseInt(s)));
          } else {
            destFieldParts.add(new Document.Segment(s));
          }
        }

      // Assign if generators contains the source field name
      String genKey = (!isBlank(sourceField) && generators.containsKey(sourceField)) ? sourceField : null;

      // Get value from source field if present, otherwise generator
      JsonNode valNode = null;
      boolean hasSource = !isBlank(sourceField) && doc.has(sourceField);
      if (hasSource) {
        valNode = doc.getJson(sourceField);
      } else if (genKey != null) {
        valNode = generateWith(genKey);
      }

      if (!hasSource && genKey == null) {
        throw new StageException("Missing value for '" + Document.Segment.stringify(destFieldParts) +
            "' (source='" + sourceField + "') and no generator available.");
      }

      if (valNode == null || valNode.isNull()) {
        log.warn("Value for '{}' resolved to null ({}).", sourceField, doc.getId());
        continue;
      }

        // Write value at the destination
        try {
          doc.setNestedJson(destFieldParts, valNode);
        } catch (ArrayIndexOutOfBoundsException | IllegalArgumentException ex) {
          throw new StageException("Failed to set field " + Document.Segment.stringify(destFieldParts) + " on doc " + doc.getId() +
              ". Field is not valid.\n" + ex.getMessage());
        }
      }
    }

    if (doc.getNestedJson(targetField) == null) {
      // set new array node on doc
      doc.setNestedJson(targetField, mapper.createArrayNode());
    }

    return null;
  }

  // Pick the number of objects
  private int pickNumObjects() {
    if (numObjects != null) {
      return numObjects;
    }

    if (minNumObjects != null) {
      return ThreadLocalRandom.current().nextInt(minNumObjects, maxNumObjects + 1);
    }

    return 1;
  }

  // Generate a value with the provided stage
  private JsonNode generateWith(String genKey) throws StageException {
    Stage gen = generators.get(genKey);

    if (gen == null) {
      return null;
    }

    // Clean up temp field
    if (genDoc.has(GEN_OUT_FIELD)) {
      genDoc.removeField(GEN_OUT_FIELD);
    }

    // Run the generator on the current doc once
    gen.processDocument(genDoc);

    return genDoc.getJson(GEN_OUT_FIELD);
  }
}