package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.UpdateMode;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import org.graalvm.polyglot.*;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;

import java.io.Reader;
import java.util.*;

/**
 * Executes a JavaScript program (via GraalVM) against each document. The stage exposes a structured
 * proxy doc for safe field access and mutation, as well as rawDoc for direct access to the underlying
 * Document. Reading a missing field from doc yields null. Assigning null to a field stores a
 * JSON null. Using the JavaScript delete operator removes fields (including nested fields and array
 * indices). Exactly one of an inline script or a script file path must be provided.
 * <br>
 * Field and container behavior:
 * <ul>
 *   <li>Root fields: Access with doc.field. Writing a JS array at the root (e.g., doc.tags = ["a","b"];)
 *   creates/overwrites a multivalued field.</li>
 *   <li>Nested objects: Use dot notation (e.g., doc.a.b). Intermediate parents are not auto-created; you
 *   must initialize them first (e.g., doc.a = doc.a || {}; doc.a.b = 1;).</li>
 *   <li>Nested arrays: Use bracket indices (e.g., doc.a.list[1]). Writing a JS array into a nested path stores a
 *   JSON array at that path (e.g., doc.payload.items = ["x","y"] becomes a JSON array).</li>
 *   <li>Deletion: delete doc.field removes a root field. delete doc.a.b removes a nested field.
 *   delete doc.a.list[1] removes the indexed element from a JSON array and compacts remaining elements.</li>
 *   <li>Types: Numbers map to int/long/double when possible. Strings, booleans, null, objects, and arrays map to
 *   their JSON or Lucille equivalents per the rules above.</li>
 * </ul>
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>script_path (String, Optional) : Path to a JavaScript file to execute. Exactly one of script_path or script must be set.</li>
 *   <li>script (String, Optional) : Inline JavaScript source to execute. Exactly one of script_path or script must be set.</li>
 *   <li>s3 (Map, Optional) : If your javascript file is held in S3. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>azure (Map, Optional) : If your javascript file is held in Azure. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>gcp (Map, Optional) : If your javascript file is held in Google Cloud. See FileConnector for the appropriate arguments to provide.</li>
 * </ul>
 */
public class ApplyJavascript extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("script_path")
      .optionalString("script")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .build();

  private final String scriptPath;
  private final String inlineScript;
  private Source source;
  private Context context;
  private Engine engine;

  private final Config config;

  public ApplyJavascript(Config config) {
    super(config);
    this.scriptPath = config.hasPath("script_path") ? config.getString("script_path") : null;
    this.inlineScript = config.hasPath("script") ? config.getString("script") : null;
    this.config = config;
  }

  @Override
  public void start() throws StageException {
    if ((scriptPath == null && inlineScript == null) || (scriptPath != null && inlineScript != null)) {
      throw new StageException("Can only specify either script path or script.");
    }

    if (inlineScript != null) {
      try {
        this.source = Source.newBuilder("js", inlineScript, "<inline>").build();
      } catch (Exception e) {
        throw new StageException("Failed to build inline JavaScript source.", e);
      }
    } else {
      try (Reader reader = FileContentFetcher.getOneTimeReader(scriptPath, config)) {
        this.source = Source.newBuilder("js", reader, scriptPath).build();
      } catch (Exception e) {
        throw new StageException("Failed to read JavaScript from '" + scriptPath + "'.", e);
      }
    }

    try {
      this.engine = Engine.newBuilder()
          .option("engine.WarnInterpreterOnly", "false")
          .option("log.level", "OFF")
          .build();

      this.context = Context.newBuilder("js")
          .engine(engine)
          .allowAllAccess(true)
          .build();
    } catch (Exception e) {
      throw new StageException("Failed to initialize JavaScript context.", e);
    }
  }

  @Override
  public void stop() throws StageException {
    if (context != null) {
      context.close();
    }

    if (engine != null) {
      engine.close();
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      context.getBindings("js").putMember("doc", JsDocProxy.root(doc));
      context.getBindings("js").putMember("rawDoc", doc);
      context.eval(source);
      return null;
    } catch (Exception e) {
      throw new StageException("JavaScript failed for doc '" + doc.getId() + "' using '" + scriptPath + "'.", e);
    }
  }

  // Proxy implementation exposed to JS

  private static final class JsDocProxy implements ProxyObject {
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private final Document doc;
    private final String path; // null for root, otherwise "a.b"

    private JsDocProxy(Document doc, String path) {
      this.doc = doc;
      this.path = path;
    }

    static JsDocProxy root(Document d) {
      return new JsDocProxy(d, null);
    }

    private boolean isRoot() {
      return path == null;
    }
    private String join(String key) {
      return isRoot() ? key : (path + "." + key);
    }

    @Override
    public Object getMember(String key) {
      return isRoot() ? getRootMember(key) : getNestedMember(key);
    }

    private Object getRootMember(String key) {
      if (!doc.has(key)) {
        return null;
      }

      JsonNode json = doc.getJson(key);

      if (json != null) {
        if (json.isNull()) return null;
        if (json.isArray()) return jsonArrayToProxyArray(json, key);
        if (json.isContainerNode()) return new JsDocProxy(doc, key);
        return jsonNodeToJsValue(json);
      }

      Object raw = doc.asMap().get(key);
      if (raw instanceof List<?>) {
        return lucilleListToProxyArray((List<?>) raw);
      }

      return lucilleScalarToJsValue(raw);
    }

    private Object getNestedMember(String key) {
      JsonNode current = doc.getNestedJson(path);
      if (current == null || current.isNull()) {
        return null;
      }

      if (current.isObject()) {
        JsonNode child = current.get(key);

        if (child == null) {
          return null;
        }

        return child.isContainerNode() ? new JsDocProxy(doc, join(key)) : jsonNodeToJsValue(child);
      }

      if (current.isArray()) {
        int idx = parseArrayIndex(key);

        if (idx < 0 || idx >= current.size()) {
          return null;
        }

        JsonNode child = current.get(idx);

        return child.isContainerNode() ? new JsDocProxy(doc, join(key)) : jsonNodeToJsValue(child);
      }
      return null;
    }

    @Override
    public void putMember(String key, Value v) {
      String full = join(key);

      if (v == null || v.isNull()) {
        if (isRoot()) {
          doc.setField(key, NullNode.getInstance());
        } else {
          doc.setNestedJson(full, NullNode.getInstance());
        }
        return;
      }

      if (v.hasArrayElements()) {
        if (isRoot()) {
          int n = (int) v.getArraySize();
          Object[] items = new Object[n];

          for (int i = 0; i < n; i++) {
            items[i] = jsValueToJavaScalar(v.getArrayElement(i));
          }

          doc.update(key, UpdateMode.OVERWRITE, items);
        } else {
          doc.setNestedJson(full, jsArrayToArrayNode(v));
        }
        return;
      }

      if (v.hasMembers()) {
        ObjectNode obj = jsObjectToObjectNode(v);
        if (isRoot()) {
          doc.setField(key, obj);
        } else {
          doc.setNestedJson(full, obj);
        }
        return;
      }

      // Scalars
      if (isRoot()) {
        doc.setField(key, jsValueToJavaScalar(v));
      } else {
        doc.setNestedJson(full, jsValueToJsonNode(v));
      }
    }

    @Override
    public boolean removeMember(String key) {
      String full = join(key);

      if (isRoot()) {
        if (!doc.has(key)) {
          return false;
        }
        
        try {
          doc.validateFieldNames(key);
        } catch (IllegalArgumentException e) {
          return false;
        }
        
        doc.removeField(key);
        return true;
      }

      if (doc.getNestedJson(full) == null) {
        return false;
      }
      
      doc.removeNestedJson(full);
      return true;
    }

    @Override
    public boolean hasMember(String key) {
      if (isRoot()) return doc.has(key);

      JsonNode current = doc.getNestedJson(path);
      if (current == null || current.isNull()) {
        return false;
      }

      if (current.isObject()) {
        return ((ObjectNode) current).has(key);
      }
      
      if (current.isArray()) {
        int idx = parseArrayIndex(key);
        return idx >= 0 && idx < current.size();
      }
      
      return false;
    }

    @Override
    public Object getMemberKeys() {
      if (isRoot()) {
        Set<String> names = doc.getFieldNames();
        return ProxyArray.fromArray(names.toArray(new String[0]));
      }

      JsonNode node = doc.getNestedJson(path);
      if (node == null || node.isNull()) {
        return ProxyArray.fromArray(new String[0]);
      }

      if (node.isObject()) {
        List<String> keys = new ArrayList<>();
        node.fieldNames().forEachRemaining(keys::add);
        return ProxyArray.fromArray(keys.toArray(new String[0]));
      }

      if (node.isArray()) {
        String[] idx = new String[node.size()];

        for (int i = 0; i < node.size(); i++) {
          idx[i] = Integer.toString(i);
        }

        return ProxyArray.fromArray(idx);
      }

      return ProxyArray.fromArray(new String[0]);
    }

    // Typing conversions

    // Document -> JS (JSON): convert JSON scalar node to a JS compatible Java value
    private static Object jsonNodeToJsValue(JsonNode node) {
      if (node.isBoolean()) return node.booleanValue();
      if (node.isNumber()) {
        if (node.canConvertToInt()) return node.intValue();
        if (node.canConvertToLong()) return node.longValue();
        return node.doubleValue();
      }
      if (node.isTextual()) return node.asText();
      return node.toString();
    }

    // Document -> JS: convert field to a JS compatible primitive/string
    private static Object lucilleScalarToJsValue(Object v) {
      if (v == null) return null;
      if (v instanceof Boolean || v instanceof Integer || v instanceof Long || v instanceof Double || v instanceof String) return v;
      return v.toString();
    }

    // Document -> JS: convert a JSON array to a ProxyArray and make containers nested proxies
    private Object jsonArrayToProxyArray(JsonNode jsonArray, String baseKey) {
      int n = jsonArray.size();
      Object[] arr = new Object[n];

      for (int i = 0; i < n; i++) {
        JsonNode child = jsonArray.get(i);
        arr[i] = child.isContainerNode() ? new JsDocProxy(doc, baseKey + "." + i) : jsonNodeToJsValue(child);
      }

      return ProxyArray.fromArray(arr);
    }

    // Document -> JS: convert a Lucille multivalued field to a ProxyArray
    private Object lucilleListToProxyArray(List<?> list) {
      Object[] arr = new Object[list.size()];

      for (int i = 0; i < list.size(); i++) {
        arr[i] = lucilleScalarToJsValue(list.get(i));
      }

      return ProxyArray.fromArray(arr);
    }

    // JS -> Document: convert JS value to Java scalar for setField/update
    private static Object jsValueToJavaScalar(Value v) {
      if (v == null || v.isNull()) return NullNode.getInstance();
      if (v.isBoolean()) return v.asBoolean();
      if (v.fitsInInt()) return v.asInt();
      if (v.fitsInLong()) return v.asLong();
      if (v.fitsInDouble()) return v.asDouble();
      if (v.isString()) return v.asString();
      return v.toString();
    }

    // JS -> Document: convert JS value to JsonNode
    private static JsonNode jsValueToJsonNode(Value v) {
      if (v == null || v.isNull()) return NullNode.getInstance();
      if (v.hasArrayElements()) return jsArrayToArrayNode(v);
      if (v.hasMembers()) return jsObjectToObjectNode(v);
      if (v.isBoolean()) return BooleanNode.valueOf(v.asBoolean());
      if (v.fitsInInt()) return IntNode.valueOf(v.asInt());
      if (v.fitsInLong()) return LongNode.valueOf(v.asLong());
      if (v.fitsInDouble()) return DoubleNode.valueOf(v.asDouble());
      if (v.isString()) return TextNode.valueOf(v.asString());
      return TextNode.valueOf(v.toString());
    }

    // JS -> Document: build ArrayNode from JS array
    private static ArrayNode jsArrayToArrayNode(Value v) {
      ArrayNode arr = JSON.arrayNode();

      for (int i = 0; i < v.getArraySize(); i++) {
        arr.add(jsValueToJsonNode(v.getArrayElement(i)));
      }

      return arr;
    }

    // JS -> Document: build ObjectNode from JS object
    private static ObjectNode jsObjectToObjectNode(Value v) {
      ObjectNode obj = JSON.objectNode();

      for (String k : v.getMemberKeys()) {
        obj.set(k, jsValueToJsonNode(v.getMember(k)));
      }

      return obj;
    }

    // Check if the key is an array index or default to -1
    private static int parseArrayIndex(String s) {
      try { return Integer.parseInt(s); } catch (NumberFormatException e) { return -1; }
    }
  }
}