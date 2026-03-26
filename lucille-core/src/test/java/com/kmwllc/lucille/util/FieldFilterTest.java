package com.kmwllc.lucille.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
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
}
