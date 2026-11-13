package com.kmwllc.lucille.salesforce.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import org.junit.Test;

public class SoqlTest {

  @Test
  public void testNormalizeWhere() {
    assertNull(Soql.normalizeWhere("  "));
    assertEquals("Industry = 'Energy'", Soql.normalizeWhere("WHERE Industry = 'Energy'"));
    assertEquals("Name = 'order by mail'", Soql.normalizeWhere("Name = 'order by mail'"));
    assertThrows(IllegalArgumentException.class, () -> Soql.normalizeWhere("Industry = 'Energy' LIMIT 1"));
    assertThrows(IllegalArgumentException.class, () -> Soql.normalizeWhere("Industry = 'Energy'; DELETE"));
  }

  @Test
  public void testSelectAndEscape() {
    assertEquals("SELECT Id, Name FROM Account WHERE Industry = 'Energy' ORDER BY Id LIMIT 2",
        Soql.select("Account", List.of("Id", "Name"), "Industry = 'Energy'", 2));
    assertEquals("SELECT Id FROM Widget__c WHERE Id IN ('a\\'b', 'c\\\\d')",
        Soql.selectByIds("Widget__c", List.of("Id"), List.of("a'b", "c\\d")));
    assertEquals("Account", Soql.referencedObject("SELECT Id FROM Account WHERE Name = 'FROM Contact'"));
    assertTrue(Soql.isIdentifier("Project__c"));
    assertFalse(Soql.isIdentifier("Bad Name"));
  }

  @Test
  public void testChunkFieldsAndIdWindows() {
    List<String> fields = new ArrayList<>();
    fields.add("Id");
    for (int i = 0; i < 12; i++) {
      fields.add(String.format("Field%02d__c", i));
    }
    List<List<String>> chunks = Soql.chunkFields("Widget__c", fields, null, null, 179, 30);
    assertTrue(chunks.size() > 1);
    assertEquals("Id", chunks.get(0).get(0));
    assertFalse(chunks.get(0).contains("Field11__c"));
    assertTrue(chunks.get(chunks.size() - 1).contains("Field11__c"));

    List<String> ids = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      ids.add("00100000000000" + i + "AAA");
    }
    String one = Soql.selectByIds("Account", List.of("Id", "Name"), List.of(ids.get(0)));
    List<String> windows = Soql.queriesForIds("Account", List.of("Id", "Name"), ids, one.length());
    assertEquals(5, windows.size());
    assertTrue(windows.get(0).contains(ids.get(0)));
  }
}
