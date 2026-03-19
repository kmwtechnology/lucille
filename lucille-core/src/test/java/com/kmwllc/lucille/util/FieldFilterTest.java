package com.kmwllc.lucille.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class FieldFilterTest {

  @Test
  public void testWhitelist() {
    Config config = ConfigFactory.parseString("whitelist = [field1, field2]");
    FieldFilter filter = new FieldFilter(config, null, null);
    assertTrue(filter.shouldInclude("field1"));
    assertTrue(filter.shouldInclude("field2"));
    assertFalse(filter.shouldInclude("field3"));
  }

  @Test
  public void testBlacklist() {
    Config config = ConfigFactory.parseString("blacklist = [field1, field2]");
    FieldFilter filter = new FieldFilter(config, null, null);
    assertFalse(filter.shouldInclude("field1"));
    assertFalse(filter.shouldInclude("field2"));
    assertTrue(filter.shouldInclude("field3"));
  }

  @Test
  public void testWhitelistKey() {
    Config config = ConfigFactory.parseString("keepFields = [field1, field2]");
    FieldFilter filter = new FieldFilter(config, "keepFields", null);
    assertTrue(filter.shouldInclude("field1"));
    assertTrue(filter.shouldInclude("field2"));
    assertFalse(filter.shouldInclude("field3"));
  }

  @Test
  public void testBlacklistKey() {
    Config config = ConfigFactory.parseString("ignoreFields = [field1, field2]");
    FieldFilter filter = new FieldFilter(config, null, "ignoreFields");
    assertFalse(filter.shouldInclude("field1"));
    assertFalse(filter.shouldInclude("field2"));
    assertTrue(filter.shouldInclude("field3"));
  }

  @Test
  public void testBothWhitelistAndBlacklist() {
    Config config = ConfigFactory.parseString(
        "keepFields = [field1, field2, field3]\n" +
            "ignoreFields = [field3]"
    );

    FieldFilter filter = new FieldFilter(config, "keepFields", "ignoreFields");
    assertTrue(filter.shouldInclude("field1"));
    assertTrue(filter.shouldInclude("field2"));
    assertFalse(filter.shouldInclude("field3"));
  }

  @Test
  public void testBlacklistIsActive() {
    Config config = ConfigFactory.parseString("blacklist = [field3]");
    FieldFilter filter = new FieldFilter(config, null, null);
    assertTrue(filter.isActive());
  }

  @Test
  public void testWhitelistIsActive() {
    Config config = ConfigFactory.parseString("whitelist = [field3]");
    FieldFilter filter = new FieldFilter(config, null, null);
    assertTrue(filter.isActive());
  }

  @Test
  public void testIsNotActive() {
    Config config = ConfigFactory.parseString(
        ""
    );
    FieldFilter filter = new FieldFilter(config, null, null);
    assertFalse(filter.isActive());
  }
}
