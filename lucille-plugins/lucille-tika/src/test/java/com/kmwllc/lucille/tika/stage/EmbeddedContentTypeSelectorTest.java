package com.kmwllc.lucille.tika.stage;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import org.apache.tika.metadata.Metadata;
import org.junit.Test;

public class EmbeddedContentTypeSelectorTest {

  private static Metadata withContentType(String contentType) {
    Metadata metadata = new Metadata();
    if (contentType != null) {
      metadata.set(Metadata.CONTENT_TYPE, contentType);
    }
    return metadata;
  }

  @Test
  public void testSkipsMatchingPrefix() {
    EmbeddedContentTypeSelector selector = new EmbeddedContentTypeSelector(List.of("image/"));
    assertFalse(selector.select(withContentType("image/png")));
    assertFalse(selector.select(withContentType("image/emf")));
  }

  @Test
  public void testParsesNonMatchingPrefix() {
    EmbeddedContentTypeSelector selector = new EmbeddedContentTypeSelector(List.of("image/"));
    assertTrue(selector.select(withContentType("application/pdf")));
    assertTrue(selector.select(withContentType("text/plain")));
  }

  @Test
  public void testNullContentTypeAlwaysParses() {
    // A null content type must be parsed regardless of prefixes, so embedded parts that never set a
    // content type (e.g. OLE native documents) aren't silently dropped.
    EmbeddedContentTypeSelector selector = new EmbeddedContentTypeSelector(List.of("image/"));
    assertTrue(selector.select(withContentType(null)));
  }

  @Test
  public void testEmptyPrefixesParsesEverything() {
    EmbeddedContentTypeSelector selector = new EmbeddedContentTypeSelector(List.of());
    assertTrue(selector.select(withContentType("image/png")));
    assertTrue(selector.select(withContentType(null)));
  }

  @Test
  public void testNullPrefixesThrows() {
    assertThrows(IllegalArgumentException.class, () -> new EmbeddedContentTypeSelector(null));
  }

  @Test
  public void testMultiplePrefixes() {
    EmbeddedContentTypeSelector selector = new EmbeddedContentTypeSelector(List.of("image/", "audio/"));
    assertFalse(selector.select(withContentType("image/jpeg")));
    assertFalse(selector.select(withContentType("audio/mpeg")));
    assertTrue(selector.select(withContentType("video/mp4")));
  }
}
