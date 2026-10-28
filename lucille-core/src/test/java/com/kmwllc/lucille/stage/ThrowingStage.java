package com.kmwllc.lucille.stage;

import com.fasterxml.jackson.core.type.TypeReference;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.util.Iterator;
import java.util.List;

/**
 * Test stage that throws a configurable Throwable, so that tests can exercise how the Worker and Pipeline handle
 * Errors as well as Exceptions.
 *
 * <ul>
 *   <li>throwable (String, Optional): class name of the Throwable to construct with a String message and throw from
 *   processDocument. Defaults to com.kmwllc.lucille.core.StageException.</li>
 *   <li>onlyForIds (List&lt;String&gt;, Optional): only throw for documents with one of these ids.</li>
 *   <li>throwOnStop (Boolean, Optional): throw a StageException from stop() instead of from processDocument.</li>
 * </ul>
 */
public class ThrowingStage extends Stage {

  public static final Spec SPEC = SpecBuilder.stage()
      .optionalString("throwable")
      .optionalBoolean("throwOnStop")
      .optionalList("onlyForIds", new TypeReference<List<String>>() {}).build();

  private final String throwableClass;
  private final List<String> onlyForIds;
  private final boolean throwOnStop;

  public ThrowingStage(Config config) {
    super(config);
    this.throwableClass = config.hasPath("throwable") ? config.getString("throwable") : StageException.class.getName();
    this.onlyForIds = config.hasPath("onlyForIds") ? config.getStringList("onlyForIds") : null;
    this.throwOnStop = config.hasPath("throwOnStop") && config.getBoolean("throwOnStop");
  }

  @Override
  public Iterator<Document> processDocument(Document doc) throws StageException {
    if (throwOnStop) {
      return null;
    }
    if (onlyForIds != null && !onlyForIds.contains(doc.getId())) {
      return null;
    }
    sneakyThrow(build());
    return null;
  }

  @Override
  public void stop() throws StageException {
    if (throwOnStop) {
      throw new StageException("Expected failure on stop");
    }
  }

  private Throwable build() {
    try {
      return (Throwable) Class.forName(throwableClass).getConstructor(String.class).newInstance("Expected (" + throwableClass + ")");
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Cannot construct " + throwableClass, e);
    }
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void sneakyThrow(Throwable t) throws T {
    throw (T) t;
  }
}
