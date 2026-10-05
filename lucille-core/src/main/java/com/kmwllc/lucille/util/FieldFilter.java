package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Standardizes whitelist and blacklist implementations for document fields. Applicable to any class which processes documents.
 */
public class FieldFilter {

  private final Set<String> whitelistSet;
  private final Set<String> blacklistSet;

  /**
   * If a whitelist or blacklist is not passed in, default to an empty list.
   * @param config the config for Lucille
   */
  public FieldFilter(Config config) {
    this.whitelistSet = readSet(config, "whitelist");
    this.blacklistSet = readSet(config, "blacklist");
  }

  // Insertion-ordered so the list getters keep the configured order.
  private static Set<String> readSet(Config config, String path) {
    return config.hasPath(path)
        ? Collections.unmodifiableSet(new LinkedHashSet<>(config.getStringList(path)))
        : Set.of();
  }

  public List<String> getWhitelist() {
    return List.copyOf(whitelistSet);
  }

  public List<String> getBlacklist() {
    return List.copyOf(blacklistSet);
  }

  public boolean isActive() {
    return !whitelistSet.isEmpty() || !blacklistSet.isEmpty();
  }

  /**
   * Returns a filtered deep copy of the given document. Fields are included or excluded according to this FieldFilter's
   * whitelist and blacklist. Reserved fields are handled via their dedicated methods {@link Document#ID_FIELD} is always
   * preserved as it is required for a valid Document.
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
    if (!whitelistSet.isEmpty() && !blacklistSet.isEmpty()) {
      return whitelistSet.contains(field) && !blacklistSet.contains(field);
    } else if (!whitelistSet.isEmpty()) {
      return whitelistSet.contains(field);
    }
    return !blacklistSet.contains(field);
  }
}
