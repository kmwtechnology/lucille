package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.databind.JsonNode;
import com.kmwllc.lucille.connector.FileConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.kmwllc.lucille.core.UpdateMode;
import com.typesafe.config.Config;
import java.util.Set;
import org.graalvm.polyglot.Engine;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.Source;
import org.graalvm.polyglot.Value;
import org.graalvm.polyglot.proxy.ProxyArray;
import org.graalvm.polyglot.proxy.ProxyObject;
import com.fasterxml.jackson.databind.node.NullNode;

import java.io.Reader;
import java.util.Iterator;

/**
 * Executes a JavaScript snippet or file against each document. The script runs once per document using GraalJS.
 * The current document is available as doc; the underlying Lucille Document is also available as rawDoc. Reading a missing field
 * returns null. Assigning null stores a JSON null. delete doc.field removes the field.
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>script (String, Optional) : Inline JavaScript to run per document. Cannot be used with script_path.</li>
 *   <li>script_path (String, Optional) : Path to the JavaScript file. If the dict_path begins with "classpath:" the classpath
 *   will be searched for the file. Otherwise, the local file system will be searched. Cannot be used with script.</li>
 *   <li>s3 (Map, Optional) : If your script is held in S3. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>azure (Map, Optional) : If your script is held in Azure. See FileConnector for the appropriate arguments to provide.</li>
 *   <li>gcp (Map, Optional) : If your script is held in Google Cloud. See FileConnector for the appropriate arguments to provide.</li>
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
  private final FileContentFetcher fileFetcher;
  private Source source;
  private Context context;

  public ApplyJavascript(Config config) {
    super(config);
    this.scriptPath = config.hasPath("script_path") ? config.getString("script_path") : null;
    this.inlineScript = config.hasPath("script") ? config.getString("script") : null;
    this.fileFetcher = new FileContentFetcher(config);
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
      try {
        fileFetcher.startup();
      } catch (Exception e) {
        throw new StageException("Error initializing FileContentFetcher.", e);
      }
      try (Reader reader = fileFetcher.getReader(scriptPath)){
        this.source = Source.newBuilder("js", reader, scriptPath).build();
      } catch (Exception e) {
        throw new StageException("Failed to read JavaScript from '" + scriptPath + "'.", e);
      }
    }

    try {
      // Disable graal logging spam.
      Engine engine = Engine.newBuilder()
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
      context = null;
    }

    if (scriptPath != null) {
      fileFetcher.shutdown();
    }
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    try {
      context.getBindings("js").putMember("doc", JsDocProxy.wrap(doc));
      context.getBindings("js").putMember("rawDoc", doc);

      context.eval(source);
      return null;
    } catch (Exception e) {
      throw new StageException("JavaScript failed for doc '" + doc.getId() + "' using '" + scriptPath + "'.", e);
    }
  }

  static final class JsDocProxy implements ProxyObject {
    private final Document doc;

    private JsDocProxy(Document doc) {
      this.doc = doc;
    }

    static JsDocProxy wrap(Document d) {
      return new JsDocProxy(d);
    }

    @Override
    public Object getMember(String key) {
      if (!doc.has(key)) {
        return null;
      }

      Object val = doc.asMap().get(key);
      if (val instanceof JsonNode && ((JsonNode) val).isNull()) {
        return null;
      }

      return val;
    }

    @Override
    public void putMember(String key, Value v) {
      if (v == null || v.isNull()) {
        doc.setField(key, NullNode.getInstance());
        return;
      }

      // Arrays
      if (v.hasArrayElements()) {
        int n = (int) v.getArraySize();
        Object[] items = new Object[n];
        for (int i = 0; i < n; i++) {
          items[i] = coerce(v.getArrayElement(i));
        }
        doc.update(key, UpdateMode.OVERWRITE, items);
        return;
      }

      // Scalars
      doc.setField(key, coerce(v));
    }

    @Override
    public boolean removeMember(String key) {
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

    private static Object coerce(Value v) {
      if (v == null || v.isNull())
        return NullNode.getInstance();
      if (v.isBoolean())
        return v.asBoolean();
      if (v.fitsInInt())
        return v.asInt();
      if (v.fitsInLong())
        return v.asLong();
      if (v.fitsInDouble())
        return v.asDouble();
      if (v.isString())
        return v.asString();

      // Resort to string otherwise
      return v.toString();
    }

    @Override
    public boolean hasMember(String key) {
      return doc.has(key);
    }

    @Override
    public Object getMemberKeys() {
      Set<String> names = doc.getFieldNames();
      return ProxyArray.fromArray(names.toArray(new String[0]));
    }
  }
}
