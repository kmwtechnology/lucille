package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.*;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
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
 *   stores a JSON array at the root.</li>
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

  public ApplyJavascript(Config config) {
    super(config);
    this.scriptPath = config.hasPath("script_path") ? config.getString("script_path") : null;
    this.inlineScript = config.hasPath("script") ? config.getString("script") : null;
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
    private final List<Document.Segment> segments; // null for root, otherwise "a.b"

    private JsDocProxy(Document doc, List<Document.Segment> segments) {
      this.doc = doc;
      this.segments = segments;
    }

    static JsDocProxy root(Document d) {
      return new JsDocProxy(d, new ArrayList());
    }

    private boolean isRoot() {
      return segments.isEmpty();
    }

    private List<Document.Segment> join(String key) {
      if (isRoot()) {
        return List.of(new Document.Segment(key));
      }
      List<Document.Segment> result = new ArrayList(segments);
      // assume an all-digit key is an array index
      if (key.chars().allMatch(c -> c >= '0' && c <= '9')) {
        result.add(new Document.Segment(Integer.parseInt(key)));
      } else {
        result.add(new Document.Segment(key));
      }
      return result;
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

      if (json.isNull()) {
        return null;
      }

      if (json.isArray()) {
        return jsonArrayToProxyArray(json, List.of(new Document.Segment(key)));
      }

      if (json.isContainerNode()) {
        return new JsDocProxy(doc, List.of(new Document.Segment(key)));
      }

      return jsonNodeToJsValue(json);
    }

    private Object getNestedMember(String key) {
      JsonNode current = doc.getNestedJson(segments);
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
      List<Document.Segment> full = join(key);

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
          doc.setField(key, jsArrayToArrayNode(v));
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
        doc.setField(key, jsValueToJsonNode(v));
      } else {
        doc.setNestedJson(full, jsValueToJsonNode(v));
      }
    }

    @Override
    public boolean removeMember(String key) {
      List<Document.Segment> full = join(key);

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
      if (isRoot()) {
        return doc.has(key);
      }

      JsonNode current = doc.getNestedJson(segments);
      if (current == null || current.isNull()) {
        return false;
      }

      if (current.isObject()) {
        return current.has(key);
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

      JsonNode node = doc.getNestedJson(segments);
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
      if (node.isBoolean()) {
        return node.booleanValue();
      }

      if (node.isNumber()) {
        if (node.canConvertToInt()) {
          return node.intValue();
        }

        if (node.canConvertToLong()) {
          return node.longValue();
        }

        return node.doubleValue();
      }

      if (node.isTextual()) {
        return node.asText();
      }

      return node.toString();
    }

    // Document -> JS: convert a JSON array to a ProxyArray and make containers nested proxies
    private Object jsonArrayToProxyArray(JsonNode jsonArray, List<Document.Segment> baseKey) {
      int n = jsonArray.size();
      Object[] arr = new Object[n];

      for (int i = 0; i < n; i++) {
        JsonNode child = jsonArray.get(i);
        if (child.isContainerNode()) {
          List<Document.Segment> extendedKey = new ArrayList(baseKey);
          extendedKey.add(new Document.Segment(i));
          arr[i] = new JsDocProxy(doc, extendedKey);
        } else {
          arr[i] = jsonNodeToJsValue(child);
        }
      }

      return ProxyArray.fromArray(arr);
    }

    // JS -> Document: convert JS value to JsonNode
    private static JsonNode jsValueToJsonNode(Value v) {
      if (v == null || v.isNull()) {
        return NullNode.getInstance();
      }

      if (v.hasArrayElements()) {
        return jsArrayToArrayNode(v);
      }

      if (v.hasMembers()) {
        return jsObjectToObjectNode(v);
      }

      if (v.isBoolean()) {
        return BooleanNode.valueOf(v.asBoolean());
      }

      if (v.fitsInInt()) {
        return IntNode.valueOf(v.asInt());
      }

      if (v.fitsInLong()) {
        return LongNode.valueOf(v.asLong());
      }

      if (v.fitsInDouble()) {
        return DoubleNode.valueOf(v.asDouble());
      }

      if (v.isString()) {
        return TextNode.valueOf(v.asString());
      }

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
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        return -1;
      }
    }
  }
}