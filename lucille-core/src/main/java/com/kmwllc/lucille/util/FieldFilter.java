package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Standardizes include and exclude lists for document fields. Applicable to any class which processes documents. When both
 * lists are set, a field passes only if it is included and not excluded.
 */
public class FieldFilter {

  private final Set<String> includeSet;
  private final Set<String> excludeSet;

  /**
   * Reads the <code>includeFields</code> and <code>excludeFields</code> lists from the given config; either may be
   * absent, which leaves it empty.
   * @param config the config holding the lists
   */
  public FieldFilter(Config config) {
    this(config, "includeFields", "excludeFields");
  }

  /**
   * Reads the include and exclude lists from the given config paths, for a component whose lists filter something
   * other than document fields (such as TextExtractor's includeMetadata and excludeMetadata). Either may be absent,
   * which leaves it empty.
   * @param config the config holding the lists
   * @param includePath the path of the include list
   * @param excludePath the path of the exclude list
   */
  public FieldFilter(Config config, String includePath, String excludePath) {
    this.includeSet = readSet(config, includePath);
    this.excludeSet = readSet(config, excludePath);
  }

  // Insertion-ordered so the list getters keep the configured order; duplicate entries are dropped.
  private static Set<String> readSet(Config config, String path) {
    return config.hasPath(path)
        ? Collections.unmodifiableSet(new LinkedHashSet<>(config.getStringList(path)))
        : Set.of();
  }

  /**
   * @return the configured include list, in configured order, without duplicates
   */
  public List<String> getIncludeFields() {
    return List.copyOf(includeSet);
  }

  /**
   * @return the configured exclude list, in configured order, without duplicates
   */
  public List<String> getExcludeFields() {
    return List.copyOf(excludeSet);
  }

  public boolean isActive() {
    return !includeSet.isEmpty() || !excludeSet.isEmpty();
  }

  /**
   * Returns a filtered deep copy of the given document. Fields are included or excluded according to this FieldFilter's
   * include and exclude lists. Reserved fields are handled via their dedicated methods; {@link Document#ID_FIELD} is always
   * preserved as it is required for a valid Document.
   *
   * <p>The result is always a new document, even when the filter is not {@link #isActive() active}, so callers may
   * add, change and remove its fields freely regardless of configuration. The one exception is byte[] values, which the
   * copy shares with the original (see {@link Document#deepCopy()}), so their contents should not be modified in place.
   * A caller that only reads the result and wants to avoid the copy when nothing would be filtered should check
   * {@link #isActive()} first and use the original document when it is false.
   *
   * @param doc the document to filter
   * @return a filtered deep copy of the document
   */
  public Document getFilteredDocument(Document doc) {
    Document copy = doc.deepCopy();

    if (!isActive()) {
      return copy;
    }

    if (!shouldInclude(Document.RUNID_FIELD)) {
      copy.clearRunId();
    }
    if (!shouldInclude(Document.DROP_FIELD)) {
      copy.setDropped(false);
    }
    if (!shouldInclude(Document.SKIP_FIELD)) {
      copy.setSkipped(false);
    }
    if (!shouldInclude(Document.CHILDREN_FIELD)) {
      copy.removeChildren();
    }

    for (String field : doc.getFieldNames()) {
      if (!Document.RESERVED_FIELDS.contains(field) && !shouldInclude(field)) {
        copy.removeField(field);
      }
    }

    return copy;
  }

  public boolean shouldInclude(String field) {
    if (!includeSet.isEmpty() && !excludeSet.isEmpty()) {
      return includeSet.contains(field) && !excludeSet.contains(field);
    } else if (!includeSet.isEmpty()) {
      return includeSet.contains(field);
    }
    return !excludeSet.contains(field);
  }
}
