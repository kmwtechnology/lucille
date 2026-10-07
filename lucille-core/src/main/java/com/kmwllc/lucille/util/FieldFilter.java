package com.kmwllc.lucille.util;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import java.util.List;

/**
 * Standardizes whitelist and blacklist implementations for document fields. Applicable to any class which processes documents.
 */
public class FieldFilter {

  private final List<String> whitelist;
  private final List<String> blacklist;

  /**
   * If a whitelist or blacklist is not passed in, default to an empty list.
   * @param config the config for Lucille
   */
  public FieldFilter(Config config) {
    this.whitelist = config.hasPath("whitelist") ? List.copyOf(config.getStringList("whitelist")) : List.of();
    this.blacklist = config.hasPath("blacklist") ? List.copyOf(config.getStringList("blacklist")) : List.of();
  }

  public List<String> getWhitelist() {
    return whitelist;
  }

  public List<String> getBlacklist() {
    return blacklist;
  }

  public boolean isActive() {
    return !whitelist.isEmpty() || !blacklist.isEmpty();
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
    if (!whitelist.isEmpty() && !blacklist.isEmpty()) {
      return whitelist.contains(field) && !blacklist.contains(field);
    } else if (!whitelist.isEmpty()) {
      return whitelist.contains(field);
    }
    return !blacklist.contains(field);
  }
}
