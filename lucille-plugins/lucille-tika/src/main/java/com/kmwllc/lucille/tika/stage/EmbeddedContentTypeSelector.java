package com.kmwllc.lucille.tika.stage;

import java.util.List;
import org.apache.tika.extractor.DocumentSelector;
import org.apache.tika.metadata.Metadata;

/**
 * A Tika {@link DocumentSelector} that skips embedded documents whose content type starts with one
 * of a configured set of prefixes. Tika consults the selector before opening each embedded part, so
 * skipping avoids the stream open, detection, and parse for that part entirely.
 *
 * <p>Driven by {@link TextExtractor}'s {@code skipEmbeddedContentTypePrefixes} config. A common use
 * is {@code ["image/"]} to skip embedded images when only text is wanted.
 *
 * <p>A null content type always returns {@code true} (parse), regardless of the configured prefixes:
 * some embedded parts (e.g. OLE 1.0 native documents) never set a content type, and skipping on null
 * would silently drop them.
 */
final class EmbeddedContentTypeSelector implements DocumentSelector {

  private final List<String> skipContentTypePrefixes;

  EmbeddedContentTypeSelector(List<String> skipContentTypePrefixes) {
    if (skipContentTypePrefixes == null) {
      throw new IllegalArgumentException("skipContentTypePrefixes must not be null");
    }
    this.skipContentTypePrefixes = List.copyOf(skipContentTypePrefixes);
  }

  @Override
  public boolean select(Metadata metadata) {
    String contentType = metadata.get(Metadata.CONTENT_TYPE);
    if (contentType == null) {
      return true;
    }
    for (String prefix : skipContentTypePrefixes) {
      if (contentType.startsWith(prefix)) {
        return false;
      }
    }
    return true;
  }
}
