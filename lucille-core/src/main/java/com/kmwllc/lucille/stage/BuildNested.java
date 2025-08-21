package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
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
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.commons.lang3.tuple.ImmutablePair;

import static org.apache.commons.lang3.StringUtils.isBlank;

// TODO: Write class level javadocs
public class BuildNested extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("target_field")
      .requiredList("entries", new TypeReference<List<String>>() {})
      .optionalBoolean("include_nulls")
      .optionalNumber("num_objects")
      .optionalNumber("min_num_objects", "max_num_objects")
      .optionalParent("generators", new TypeReference<Map<String, Object>>() {})
      .build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String targetField;
  private final List<String> entries;
  private final List<Entry<String[], String>> parsedEntries;
  private final boolean includeNulls;
  private final Integer numObjects; // fixed N (optional)
  private final Integer minNumObjects; // range min (optional)
  private final Integer maxNumObjects; // range max (optional)

  private final Map<String, Stage> generators = new LinkedHashMap<>();
  private final Map<String, String> genOutField = new LinkedHashMap<>();
  private final Config generatorsConfig;

  public BuildNested(Config config) throws StageException {
    super(config);
    this.targetField = ConfigUtils.getOrDefault(config, "target_field", null);
    this.entries  = config.getStringList("entries");
    this.includeNulls = ConfigUtils.getOrDefault(config, "include_nulls", false);
    this.numObjects = ConfigUtils.getOrDefault(config, "num_objects", null);
    this.minNumObjects = ConfigUtils.getOrDefault(config, "min_num_objects", null);
    this.maxNumObjects = ConfigUtils.getOrDefault(config, "max_num_objects", null);
    this.generatorsConfig = config.hasPath("generators") ? config.getConfig("generators") : null;

    if (targetField == null || targetField.isEmpty()) {
      throw new StageException("target_field is required.");
    }
    if (Document.RESERVED_FIELDS.contains(targetField)) {
      throw new StageException("target_field '" + targetField + "' is a reserved field");
    }
    if (entries == null || entries.isEmpty()) {
      throw new StageException("entries must be a non-empty list of 'nested.path=source_field' strings (source_field may be empty).");
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

    this.parsedEntries = Collections.unmodifiableList(parseEntries(this.entries));
  }

  // Start generator stages
  @Override
  public void start() throws StageException {
    if (generatorsConfig == null) {
      return;
    }

    // Copy generator object into map
    Map<String, Object> raw = new LinkedHashMap<>(generatorsConfig.root().unwrapped());
    // Iterate over each generator param
    for (Map.Entry<String, Object> entry : raw.entrySet()) {
      String key = entry.getKey();
      Config sub = generatorsConfig.getConfig(key);
      if (!sub.hasPath("class")) {
        throw new StageException("generators." + key + " must include a 'class' property");
      }

      // Inline default temp field name
      String tmpField = sub.hasPath("field_name") ? sub.getString("field_name") : ".bn_gen." + key;

      // Add default name and field name
      Config injected = sub;
      if (!sub.hasPath("field_name")) {
        injected = ConfigFactory.parseMap(Map.of(
            "name", "bn_gen_" + key,
            "field_name", tmpField
        )).withFallback(sub);
      }

      // Create generator
      Stage gen = instantiateStage(injected);
      gen.start();
      generators.put(key, gen);
      genOutField.put(key, tmpField);
    }
  }

  @Override
  public void stop() throws StageException {
    for (Stage generator : generators.values()) {
      try {
        generator.stop();
      } catch (Exception ignored) {}
    }
  }

  // TODO: This will be changed once entries is updated to a mapping instead of using =
  private static List<Entry<String[], String>> parseEntries(List<String> entries) throws StageException {
    List<Entry<String[], String>> out = new ArrayList<>(entries.size());

    // Iterate over each entry
    for (String raw : entries) {
      String m = (raw == null) ? "" : raw.trim();

      if (m.isEmpty()) {
        continue;
      }

      // Ensure that the entry uses an = to assign a target and source field
      int eq = m.indexOf('=');

      if (eq < 0) {
        throw new StageException("Invalid mapping: '" + raw + "'. Expected 'nested.path=source_field' (source_field may be empty).");
      }

      // Split up the destination and source fields
      String destPath = m.substring(0, eq).trim();
      String source   = m.substring(eq + 1).trim();

      if (destPath.isEmpty()) {
        throw new StageException("Invalid mapping (empty destination path): '" + raw + "'");
      }

      // Break the destination into parts of a path at .
      String[] parts = destPath.split("\\.");
      out.add(new ImmutablePair<>(parts, source));
    }

    return out;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    // Get doc as map so we can use previous field values
    Map<String, Object> map = doc.asMap();
    final int n = pickNumObjects();
    ArrayNode arr = MAPPER.createArrayNode();

    // Iterate over each nested object
    for (int i = 0; i < n; i++) {
      ObjectNode entity = MAPPER.createObjectNode();
      boolean wroteAny = false;

      // Iterate over each entry for the object
      for (Entry<String[], String> mp : parsedEntries) {
        String[] destParts = mp.getKey();
        String sourceField = mp.getValue();

        // Assign if generators contains the source field name
        String genKey = (!isBlank(sourceField) && generators.containsKey(sourceField)) ? sourceField : null;

        // Get value from source field if present, otherwise generator
        Object val = null;
        if (!isBlank(sourceField) && doc.has(sourceField)) {
          val = map.get(sourceField);
        } else if (genKey != null) {
          val = generateWith(genKey, doc);
        }

        if (val == null && genKey == null) {
          String destPathStr = String.join(".", destParts);
          throw new StageException("Missing value for '" + destPathStr +
              "' (source='" + sourceField + "') and no generator available.");
        }

        // Explicit null from an existing field
        if (val == null && !includeNulls) {
          continue;
        }

        // Write value at the destination
        JsonNode node = MAPPER.valueToTree(val);
        setNested(entity, destParts, node);
        wroteAny = true;
      }

      if (wroteAny || includeNulls) {
        arr.add(entity);
      }
    }

    doc.setField(targetField, arr);
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
  private Object generateWith(String genKey, Document doc) throws StageException {
    Stage gen = generators.get(genKey);

    if (gen == null) {
      return null;
    }

    // Field that the generator writes to
    String outField = genOutField.get(genKey);

    // Run the generator on the current doc once
    gen.processDocument(doc);

    Object value = doc.asMap().get(outField);

    // Clean up temp field
    if (outField != null && doc.has(outField)) {
      doc.removeField(outField);
    }

    return value;
  }

  // Create a dotted path inside the root and set the leaf to value
  private void setNested(ObjectNode root, String[] parts, JsonNode value) throws StageException {
    if (parts == null || parts.length == 0) {
      throw new StageException("Destination path cannot be empty.");
    }

    // Start at the root
    ObjectNode cur = root;
    // Iterate over all parent segments in the path
    for (int i = 0; i < parts.length - 1; i++) {
      String key = parts[i];
      JsonNode existing = cur.get(key);

      // Create the node if it doesn't exist, otherwise just move down to that level
      if (!(existing instanceof ObjectNode)) {
        ObjectNode next = MAPPER.createObjectNode();
        cur.set(key, next);
        cur = next;
      } else {
        cur = (ObjectNode) existing;
      }
    }

    // Write the value at the leaf
    cur.set(parts[parts.length - 1], value);
  }

  // Use reflection to create generator stage
  private static Stage instantiateStage(Config cfg) throws StageException {
    try {
      String cls = cfg.getString("class");
      Class<?> k = Class.forName(cls);

      if (!Stage.class.isAssignableFrom(k)) {
        throw new StageException("Generator class is not a Stage: " + cls);
      }

      return (Stage) k.getConstructor(Config.class).newInstance(cfg);
    } catch (Exception e) {
      throw new StageException("Failed to instantiate generator stage", e);
    }
  }
}