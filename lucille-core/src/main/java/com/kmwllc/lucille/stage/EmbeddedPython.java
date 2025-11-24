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
import org.graalvm.polyglot.proxy.ProxyHashMap;
import org.graalvm.polyglot.proxy.ProxyIterator;

import java.io.Reader;
import java.util.*;

/**
 * Executes user-provided Python code against each Document using an embedded GraalPy interpreter. Python scripts interact
 * with the Document through a proxy object that behaves like both a Python dictionary and an object with attributes. This
 * dual interface is intentional and mirrors common Python patterns: attribute-style access (`doc.field`) for convenience, and
 * dictionary-style access (`doc["field"]`) for dynamic or non-identifier field names. Both forms operate on the same
 * underlying data.
 * <p>
 * Config Parameters:
 * <ul>
 *   <li>script_path (String, Optional) : Path to the Python script to run.</li>
 *   <li>script (String, Optional) : Inline Python code to execute instead of a file.</li>
 *   <li>s3 (Map, Optional) : If your Python file is held in S3. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>azure (Map, Optional) : If your javascript file is held in Azure. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>gcp (Map, Optional) : If your javascript file is held in Google Cloud. See FileConnector for the appropriate arguments to provide.</li>
 * </ul>
 */
public class EmbeddedPython extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("script_path", "script")
      .optionalParent(FileConnector.S3_PARENT_SPEC, FileConnector.GCP_PARENT_SPEC, FileConnector.AZURE_PARENT_SPEC)
      .build();

  private final String scriptPath;
  private final String inlineScript;
//  private final String pythonRoot;

  private Source source;
  private Context context;
  private Engine engine;

  public EmbeddedPython(Config config) {
    super(config);
    this.scriptPath = config.hasPath("script_path") ? config.getString("script_path") : null;
    this.inlineScript = config.hasPath("script") ? config.getString("script") : null;
//    this.pythonRoot = config.hasPath("python_root") ? config.getString("python_root") : null;
  }

  @Override
  public void start() throws StageException {
    if ((scriptPath == null && inlineScript == null) || (scriptPath != null && inlineScript != null)) {
      throw new StageException("Can only specify either script path or script.");
    }

    if (inlineScript != null) {
      try {
        this.source = Source.newBuilder("python", inlineScript, "<inline>").build();
      } catch (Exception e) {
        throw new StageException("Failed to build inline Python source.", e);
      }
    } else {
      try (Reader reader = FileContentFetcher.getOneTimeReader(scriptPath, config)) {
        this.source = Source.newBuilder("python", reader, scriptPath).build();
      } catch (Exception e) {
        throw new StageException("Failed to read Python from '" + scriptPath + "'.", e);
      }
    }

    try {
      this.engine = Engine.newBuilder()
          .option("engine.WarnInterpreterOnly", "false")
          .option("log.level", "OFF")
          .build();

      this.context = Context.newBuilder("python")
          .engine(engine)
          .allowAllAccess(true)
          .build();

      // Code for Python dependencies, but requires JDK 21

//      if (pythonRoot != null) {
//        Path pyRoot = Paths.get(pythonRoot);
//        this.context = GraalPyResources.contextBuilder(pyRoot)
//            .engine(engine)
//            .out(System.out)
//            .err(System.err)
//            .build();
//      } else {
//        this.context = Context.newBuilder("python")
//            .engine(engine)
//            .allowAllAccess(true)
//            .out(System.out)
//            .err(System.err)
//            .build();
//      }
    } catch (Exception e) {
      throw new StageException("Failed to initialize Python context.", e);
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
      context.getBindings("python").putMember("doc", PyDocProxy.root(doc));
      context.getBindings("python").putMember("rawDoc", doc);
      context.eval(source);
      return null;
    } catch (Exception e) {
      throw new StageException("Python failed for doc '" + doc.getId() + "' using '" + scriptPath + "'.", e);
    }
  }

  // Proxy implementation exposed to Python

  private static final class PyDocProxy implements ProxyObject, ProxyHashMap {
    private static final JsonNodeFactory JSON = JsonNodeFactory.instance;

    private final Document doc;
    private final List<Document.Segment> segments;

    private PyDocProxy(Document doc, List<Document.Segment> segments) {
      this.doc = doc;
      this.segments = segments;
    }

    static PyDocProxy root(Document d) {
      return new PyDocProxy(d, new ArrayList());
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
      Object val = isRoot() ? getRootMember(key) : getNestedMember(key);
      if (val == null) {
        if (hasMember(key)) {
          return null;
        }

        throw new IllegalArgumentException("No member found for key '" + key + "'");
      }
      return val;
    }

    private Object getRootMember(String key) {
      if (!doc.has(key)) {
        return null;
      }

      JsonNode json = doc.getJson(key);

      if (json == null || json.isNull()) {
        return null;
      }

      if (json.isArray()) {
        return new PyArrayProxy(doc, List.of(new Document.Segment(key)));
      }

      if (json.isContainerNode()) {
        return new PyDocProxy(doc, List.of(new Document.Segment(key)));
      }

      return jsonNodeToPyValue(json);
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

        if (child.isArray()) {
          return new PyArrayProxy(doc, join(key));
        }

        return child.isContainerNode() ? new PyDocProxy(doc, join(key)) : jsonNodeToPyValue(child);
      }

      if (current.isArray()) {
        int idx = parseArrayIndex(key);

        if (idx < 0 || idx >= current.size()) {
          return null;
        }

        JsonNode child = current.get(idx);

        if (child.isArray()) {
          return new PyArrayProxy(doc, join(key));
        }

        return child.isContainerNode() ? new PyDocProxy(doc, join(key)) : jsonNodeToPyValue(child);
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
          doc.setField(key, pyArrayToArrayNode(v));
        } else {
          doc.setNestedJson(full, pyArrayToArrayNode(v));
        }
        return;
      }

      if (v.hasMembers()) {
        ObjectNode obj = pyObjectToObjectNode(v);
        if (isRoot()) {
          doc.setField(key, obj);
        } else {
          doc.setNestedJson(full, obj);
        }
        return;
      }

      // Scalars
      if (isRoot()) {
        doc.setField(key, pyValueToJsonNode(v));
      } else {
        doc.setNestedJson(full, pyValueToJsonNode(v));
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

    @Override
    public long getHashSize() {
      if (isRoot()) {
        return doc.getFieldNames().size();
      }

      JsonNode node = doc.getNestedJson(segments);
      if (node == null || node.isNull()) {
        return 0;
      }

      if (node.isObject() || node.isArray()) {
        return node.size();
      }

      return 0;
    }

    @Override
    public boolean hasHashEntry(Value key) {
      String k = keyToString(key);
      return hasMember(k);
    }

    @Override
    public Object getHashValue(Value key) {
      String k = keyToString(key);
      return getMember(k);
    }

    @Override
    public void putHashEntry(Value key, Value value) {
      String k = keyToString(key);
      putMember(k, value);
    }

    @Override
    public boolean removeHashEntry(Value key) {
      String k = keyToString(key);
      return removeMember(k);
    }

    @Override
    public Object getHashEntriesIterator() {
      List<String> keys = new ArrayList<>();

      if (isRoot()) {
        keys.addAll(doc.getFieldNames());
      } else {
        JsonNode node = doc.getNestedJson(segments);
        if (node != null && !node.isNull()) {
          if (node.isObject()) {
            node.fieldNames().forEachRemaining(keys::add);
          } else if (node.isArray()) {
            for (int i = 0; i < node.size(); i++) {
              keys.add(Integer.toString(i));
            }
          }
        }
      }

      List<Object> entries = new ArrayList<>(keys.size());
      for (String k : keys) {
        entries.add(ProxyArray.fromArray(k, getMember(k)));
      }

      return ProxyIterator.from(entries.iterator());
    }

    // Typing conversions

    // Document -> Python (JSON): convert JSON scalar node to a Python compatible Java value
    private static Object jsonNodeToPyValue(JsonNode node) {
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

    // Python -> Document: build ArrayNode from Python list
    private static ArrayNode pyArrayToArrayNode(Value v) {
      ArrayNode arr = JSON.arrayNode();

      for (int i = 0; i < v.getArraySize(); i++) {
        arr.add(pyValueToJsonNode(v.getArrayElement(i)));
      }

      return arr;
    }

    // Python -> Document: build ObjectNode from Python object
    private static ObjectNode pyObjectToObjectNode(Value v) {
      ObjectNode obj = JSON.objectNode();

      if (v.canInvokeMember("items")) {
        Value items = v.invokeMember("items");

        if (items.hasArrayElements()) {
          long n = items.getArraySize();

          for (int i = 0; i < n; i++) {
            Value pair = items.getArrayElement(i);
            String keyStr = keyToString(pair.getArrayElement(0));
            Value val = pair.getArrayElement(1);
            obj.set(keyStr, pyValueToJsonNode(val));
          }

          return obj;
        }

        Value it = items.invokeMember("__iter__");
        while (true) {
          Value pair;

          try {
            pair = it.invokeMember("__next__");
          } catch (PolyglotException pe) {
            if (isStopIteration(pe)) {
              break;
            }

            throw pe;
          }

          String keyStr = keyToString(pair.getArrayElement(0));
          Value val = pair.getArrayElement(1);
          obj.set(keyStr, pyValueToJsonNode(val));
        }

        return obj;
      }

      for (String k : v.getMemberKeys()) {
        if (k.startsWith("__") && k.endsWith("__")) {
          continue;
        }

        obj.set(k, pyValueToJsonNode(v.getMember(k)));
      }

      return obj;
    }

    private static String keyToString(Value v) {
        if (v.isString()) {
          return v.asString();
        }

        if (v.isHostObject()) {
          Object o = v.as(Object.class);
          return String.valueOf(o);
        }

        return String.valueOf(v);
    }

    private static boolean isStopIteration(PolyglotException e) {
      Value guest = e.getGuestObject();

      if (guest != null) {
        Value meta = guest.getMetaObject();
        String ms = (meta != null ? meta.toString() : "");
        if (ms != null && ms.contains("StopIteration")) {
          return true;
        }
      }

      String msg = e.getMessage();
      return msg != null && msg.contains("StopIteration");
    }

    private static JsonNode pyValueToJsonNode(Value v) {
      if (v == null || v.isNull()) {
        return NullNode.getInstance();
      }

      if (v.hasArrayElements()) {
        return pyArrayToArrayNode(v);
      }

      if (v.hasMembers()) {
        return pyObjectToObjectNode(v);
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

    private static int parseArrayIndex(String s) {
      try {
        return Integer.parseInt(s);
      } catch (NumberFormatException e) {
        return -1;
      }
    }
  }

  private static final class PyArrayProxy implements ProxyArray {
    private final Document doc;
    private final List<Document.Segment> path;

    PyArrayProxy(Document doc, List<Document.Segment> path) {
      this.doc = doc;
      this.path = path;
    }

    @Override
    public long getSize() {
      JsonNode node = doc.getNestedJson(path);
      return (node != null && node.isArray()) ? node.size() : 0;
    }

    @Override
    public Object get(long index) {
      JsonNode node = doc.getNestedJson(path);

      if (node == null || !node.isArray()) {
        return null;
      }

      ArrayNode arr = (ArrayNode) node;

      int i = (int) index;

      if (i < 0 || i >= arr.size()) {
        return null;
      }

      JsonNode child = arr.get(i);
      if (child.isArray()) {
        List<Document.Segment> extended = new ArrayList<>(path);
        extended.add(new Document.Segment(i));
        return new PyArrayProxy(doc, extended);
      }

      if (child.isContainerNode()) {
        List<Document.Segment> extended = new ArrayList<>(path);
        extended.add(new Document.Segment(i));
        return new PyDocProxy(doc, extended);
      }

      return PyDocProxy.jsonNodeToPyValue(child);
    }

    @Override
    public void set(long index, Value value) {
      JsonNode node = doc.getNestedJson(path);
      if (node == null || !node.isArray()) {
        throw new IllegalStateException("Expected array at " + path);
      }
      ArrayNode arr = (ArrayNode) node;

      int i = (int) index;
      if (i < 0 || i >= arr.size()) {
        throw new ArrayIndexOutOfBoundsException(i);
      }

      arr.set(i, PyDocProxy.pyValueToJsonNode(value));
      doc.setNestedJson(path, arr);
    }

    @Override
    public boolean remove(long index) {
      JsonNode node = doc.getNestedJson(path);
      if (node == null || !node.isArray()) {
        throw new IllegalStateException("Expected array at " + path);
      }
      ArrayNode arr = (ArrayNode) node;

      int i = (int) index;
      if (i < 0 || i >= arr.size()) {
        return false;
      }

      ArrayNode newArr = JsonNodeFactory.instance.arrayNode();
      for (int k = 0; k < arr.size(); k++) {
        if (k != i) {
          newArr.add(arr.get(k));
        }
      }

      doc.setNestedJson(path, newArr);
      return true;
    }
  }
}