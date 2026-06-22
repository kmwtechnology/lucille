package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Map;
import org.junit.Test;

public class FieldFilterTest {

  @Test
  public void testWhitelist() {
    Config config = ConfigFactory.parseString("whitelist = [field1, field2]");
    FieldFilter filter = new FieldFilter(config);
    assertTrue(filter.shouldInclude("field1"));
    assertTrue(filter.shouldInclude("field2"));
    assertFalse(filter.shouldInclude("field3"));
  }

  @Test
  public void testBlacklist() {
    Config config = ConfigFactory.parseString("blacklist = [field1, field2]");
    FieldFilter filter = new FieldFilter(config);
    assertFalse(filter.shouldInclude("field1"));
    assertFalse(filter.shouldInclude("field2"));
    assertTrue(filter.shouldInclude("field3"));
  }

  @Test
  public void testGetters() {
    Config config = ConfigFactory.parseString(
        "whitelist = [field1, field2, field3]\n" +
            "blacklist = [field3]"
    );

    FieldFilter filter = new FieldFilter(config);
    assertEquals(List.of("field1", "field2", "field3"), filter.getWhitelist());
    assertEquals(List.of("field3"), filter.getBlacklist());

    assertThrows(Exception.class, () -> filter.getWhitelist().add("field100"));
    assertEquals(List.of("field1", "field2", "field3"), filter.getWhitelist());

    assertThrows(Exception.class, () -> filter.getBlacklist().add("field200"));
    assertEquals(List.of("field1", "field2", "field3"), filter.getWhitelist());
  }

  @Test
  public void testBothWhitelistAndBlacklist() {
    Config config = ConfigFactory.parseString(
        "whitelist = [field1, field2, field3]\n" +
            "blacklist = [field3]"
    );

    FieldFilter filter = new FieldFilter(config);
    assertTrue(filter.shouldInclude("field1"));
    assertTrue(filter.shouldInclude("field2"));
    assertFalse(filter.shouldInclude("field3"));

    // field4 should not be included because it is not in the whitelist
    assertFalse(filter.shouldInclude("field4"));
  }

  @Test
  public void testBlacklistIsActive() {
    Config config = ConfigFactory.parseString("blacklist = [field3]");
    FieldFilter filter = new FieldFilter(config);
    assertTrue(filter.isActive());
  }

  @Test
  public void testWhitelistIsActive() {
    Config config = ConfigFactory.parseString("whitelist = [field3]");
    FieldFilter filter = new FieldFilter(config);
    assertTrue(filter.isActive());
  }

  @Test
  public void testIsNotActive() {
    Config config = ConfigFactory.parseString(
        ""
    );
    FieldFilter filter = new FieldFilter(config);
    assertFalse(filter.isActive());
  }

  @Test
  public void testGetFilteredDocumentBlacklist() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("field2"))));
    Document doc = Document.create("id1");
    doc.setField("field1", "val1");
    doc.setField("field2", "val2");

    Document filtered = filter.getFilteredDocument(doc);

    assertEquals("id1", filtered.getId());
    assertEquals("val1", filtered.getString("field1"));
    assertFalse(filtered.has("field2"));
  }

  @Test
  public void testGetFilteredDocumentWhitelist() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("whitelist", List.of("field1"))));
    Document doc = Document.create("id1");
    doc.setField("field1", "val1");
    doc.setField("field2", "val2");

    Document filtered = filter.getFilteredDocument(doc);

    assertEquals("id1", filtered.getId());
    assertEquals("val1", filtered.getString("field1"));
    assertFalse(filtered.has("field2"));
  }

  @Test
  public void testGetFilteredDocumentRunIdBlacklisted() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("run_id"))));
    Document doc = Document.create("id1", "run1");

    Document filtered = filter.getFilteredDocument(doc);

    assertEquals("id1", filtered.getId());
    assertNull(filtered.getRunId());
  }

  @Test
  public void testGetFilteredDocumentRunIdWhitelisted() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("whitelist", List.of("run_id"))));
    Document doc = Document.create("id1", "run1");
    doc.setField("field1", "val1");

    Document filtered = filter.getFilteredDocument(doc);

    assertEquals("id1", filtered.getId());
    assertEquals("run1", filtered.getRunId());
    assertFalse(filtered.has("field1"));
  }

  @Test
  public void testGetFilteredDocumentDroppedClearedWhenNotInWhitelist() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("whitelist", List.of("field1"))));
    Document doc = Document.create("id1");
    doc.setField("field1", "val1");
    doc.setDropped(true);

    Document filtered = filter.getFilteredDocument(doc);

    assertFalse(filtered.isDropped());
    assertTrue(doc.isDropped());
  }

  @Test
  public void testGetFilteredDocumentChildrenRemovedWhenBlacklisted() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("___children"))));
    Document doc = Document.create("id1");
    doc.addChild(Document.create("child1"));

    Document filtered = filter.getFilteredDocument(doc);

    assertFalse(filtered.hasChildren());
    assertTrue(doc.hasChildren());
  }

  @Test
  public void testGetFilteredDocumentOriginalUnchanged() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.parseMap(Map.of("blacklist", List.of("field2"))));
    Document doc = Document.create("id1");
    doc.setField("field1", "val1");
    doc.setField("field2", "val2");

    Document filteredDoc = filter.getFilteredDocument(doc);
    filteredDoc.setField("field3", "val3");

    assertEquals("val1", doc.getString("field1"));
    assertEquals("val2", doc.getString("field2"));
    assertFalse(doc.has("field3"));
  }

  @Test
  public void testGetFilteredDocumentInactiveFilter() throws Exception {
    FieldFilter filter = new FieldFilter(ConfigFactory.empty());
    Document doc = Document.create("id1");
    doc.setField("field1", "val1");

    Document filtered = filter.getFilteredDocument(doc);

    assertEquals("id1", filtered.getId());
    assertEquals("val1", filtered.getString("field1"));
  }
}
