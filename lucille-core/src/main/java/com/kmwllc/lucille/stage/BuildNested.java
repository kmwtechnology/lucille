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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.apache.commons.lang3.StringUtils.isBlank;

public class BuildNested extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .requiredString("target_field")
      .requiredList("entries", new TypeReference<List<String>>() {})
      .optionalBoolean("include_nulls")
      .optionalNumber("num_objects")
      // Map<String, Object> of generator blocks
      .optionalParent("generators", new TypeReference<Map<String, Object>>() {})
      .build();

  private static final ObjectMapper MAPPER = new ObjectMapper();

  private final String targetField;
  private final List<String> entries;
  private final List<Entry<String[], String>> parsedEntries; // dest path pre-split once; value is sourceField (can be "")
  private final boolean includeNulls;
  private final Integer numObjects;

  // Generators: key -> Stage instance
  private final Map<String, Stage> generators = new LinkedHashMap<>();
  // Generator output field for each key (injected or user-provided)
  private final Map<String, String> genOutField = new LinkedHashMap<>();
  private final Config generatorsConfig;

  public BuildNested(Config config) throws StageException {
    super(config);
    this.targetField = ConfigUtils.getOrDefault(config, "target_field", null);
    this.entries = config.getStringList("entries");
    this.includeNulls = ConfigUtils.getOrDefault(config, "include_nulls", false);
    this.numObjects = ConfigUtils.getOrDefault(config, "num_objects", null);
    this.generatorsConfig = config.hasPath("generators") ? config.getConfig("generators") : null;

    if (targetField == null || targetField.isEmpty()) {
      throw new StageException("target_field is required.");
    }
    if (entries == null || entries.isEmpty()) {
      throw new StageException("entries must be a non-empty list of 'nested.path=source_field' strings (source_field may be empty).");
    }
    if (numObjects != null && numObjects <= 0) {
      throw new StageException("num_objects must be a positive integer if provided.");
    }

    // Pre-parse "dest=source" and pre-split dest path once up-front
    this.parsedEntries = Collections.unmodifiableList(parseEntries(this.entries));
  }

  @Override
  public void start() throws StageException {
    // Initialize configured generators (if any), injecting a unique temp field_name if not provided
    if (generatorsConfig == null) return;

    // Keep insertion order stable
    Map<String, Object> raw = new LinkedHashMap<>(generatorsConfig.root().unwrapped());
    for (Map.Entry<String, Object> e : raw.entrySet()) {
      String key = e.getKey();
      Config sub = generatorsConfig.getConfig(key);
      if (!sub.hasPath("class")) {
        throw new StageException("generators." + key + " must include a 'class' property");
      }

      // Inline default temp field name; respect user-provided field_name if present
      String tmpField = sub.hasPath("field_name") ? sub.getString("field_name") : ".bn_gen." + key;

      Config injected = sub;
      if (!sub.hasPath("field_name")) {
        injected = ConfigFactory.parseMap(Map.of(
            "name", "bn_gen_" + key,
            "field_name", tmpField
        )).withFallback(sub);
      }

      Stage gen = instantiateStage(injected);
      gen.start();
      generators.put(key, gen);
      genOutField.put(key, tmpField);
    }
  }

  @Override
  public void stop() throws StageException {
    for (Stage g : generators.values()) {
      try { g.stop(); } catch (Exception ignored) {}
    }
  }

  private static List<Entry<String[], String>> parseEntries(List<String> entries) throws StageException {
    List<Entry<String[], String>> out = new ArrayList<>(entries.size());
    for (String raw : entries) {
      String m = (raw == null) ? "" : raw.trim();
      if (m.isEmpty()) continue;

      int eq = m.indexOf('=');
      if (eq < 0) {
        throw new StageException("Invalid mapping: '" + raw + "'. Expected 'nested.path=source_field' (source_field may be empty).");
      }
      String destPath = m.substring(0, eq).trim();
      String source = m.substring(eq + 1).trim(); // may be empty
      if (destPath.isEmpty()) {
        throw new StageException("Invalid mapping (empty destination path): '" + raw + "'");
      }
      String[] parts = destPath.split("\\."); // regex used once at startup
      out.add(new AbstractMap.SimpleImmutableEntry<>(parts, source));
    }
    return out;
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    Map<String, Object> map = doc.asMap(); // generic types

    final int n = (numObjects != null) ? numObjects : 1;

    ArrayNode arr = MAPPER.createArrayNode();

    for (int i = 0; i < n; i++) {
      ObjectNode entity = MAPPER.createObjectNode();
      boolean wroteAny = false;

      for (Entry<String[], String> mp : parsedEntries) {
        String[] destParts = mp.getKey();
        String sourceField = mp.getValue(); // may be blank

        String leaf = destParts[destParts.length - 1];
        String genKey = (!isBlank(sourceField) && generators.containsKey(sourceField)) ? sourceField
            : (generators.containsKey(leaf) ? leaf : null);

        boolean hasSource = !isBlank(sourceField) && doc.has(sourceField);
        Object raw = hasSource ? map.get(sourceField) : null;

        boolean missing = false;
        Object val = null;

        if (!hasSource) {
          missing = true;
        } else if (raw instanceof List<?>) {
          List<?> L = (List<?>) raw;
          if (i < L.size()) {
            val = L.get(i);
          } else {
            missing = true; // list too short for this index
          }
        } else {
          val = raw; // scalar (may be null)
        }

        if (missing) {
          // missing value must be satisfied by a generator, else hard fail
          if (genKey != null) {
            val = generateWith(genKey, doc);
          }
          if (val == null) {
            String destPathStr = String.join(".", destParts);
            throw new StageException("Missing value for '" + destPathStr +
                "' (source='" + sourceField + "') and no generator available.");
          }
        }

        // explicit null from an existing field
        if (val == null && !includeNulls) continue;

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

  /** Run the configured generator for this key once, read its output field, then remove it. */
  private Object generateWith(String genKey, Document doc) throws StageException {
    Stage gen = generators.get(genKey);
    if (gen == null) {
      return null;
    }
    String outField = genOutField.get(genKey);

    // Run the generator on the doc once; it writes to outField
    gen.processDocument(doc);

    Object produced;
    Object raw = doc.asMap().get(outField); // read after generator ran
    if (raw instanceof List) {
      List<?> L = (List<?>) raw;
      produced = L.isEmpty() ? null : L.get(L.size() - 1); // take the last value written
    } else {
      produced = raw;
    }

    // Clean up temp field to avoid leaking into final doc
    if (outField != null && doc.has(outField)) {
      doc.removeField(outField);
    }
    return produced;
  }

  private void setNested(ObjectNode root, String[] parts, JsonNode value) throws StageException {
    if (parts == null || parts.length == 0) {
      throw new StageException("Destination path cannot be empty.");
    }
    ObjectNode cur = root;
    for (int i = 0; i < parts.length - 1; i++) {
      String key = parts[i];
      JsonNode existing = cur.get(key);
      if (!(existing instanceof ObjectNode)) {
        ObjectNode next = MAPPER.createObjectNode();
        cur.set(key, next);
        cur = next;
      } else {
        cur = (ObjectNode) existing;
      }
    }
    cur.set(parts[parts.length - 1], value);
  }

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