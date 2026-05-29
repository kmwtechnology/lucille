package com.kmwllc.lucille.postgres.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

public class PostgresIndexerTest {

  private PostgresIndexer buildBypass(String confResource) throws Exception {
    Config config = ConfigFactory.load(confResource);
    return new PostgresIndexer(config, new TestMessenger(), true, "test");
  }

  @Test
  public void testUpsertSqlGeneration() throws Exception {
    PostgresIndexer indexer = buildBypass("PostgresIndexerTest/good-config.conf");
    String sql = indexer.getInsertSql();
    assertEquals(
        "INSERT INTO \"documents\" (\"id\", \"title\", \"body\", \"embedding\") "
            + "VALUES (?, ?, ?, ?) "
            + "ON CONFLICT (\"id\") DO UPDATE SET "
            + "\"title\" = EXCLUDED.\"title\", "
            + "\"body\" = EXCLUDED.\"body\", "
            + "\"embedding\" = EXCLUDED.\"embedding\"",
        sql);
    assertTrue(indexer.validateConnection()); // bypass returns true
  }

  @Test
  public void testInsertOnlySqlGeneration() throws Exception {
    PostgresIndexer indexer = buildBypass("PostgresIndexerTest/insert-only.conf");
    String sql = indexer.getInsertSql();
    assertEquals(
        "INSERT INTO \"documents\" (\"id\", \"title\") VALUES (?, ?)",
        sql);
  }

  @Test
  public void testBypassModeSkipsIndexing() throws Exception {
    PostgresIndexer indexer = buildBypass("PostgresIndexerTest/good-config.conf");
    Document doc = Document.create("doc1");
    doc.setField("title", "hello");

    Set<Pair<Document, String>> failed = indexer.sendToIndex(List.of(doc));
    assertTrue(failed.isEmpty());
  }

  @Test
  public void testVectorColumnRequiresDim() {
    try {
      buildBypass("PostgresIndexerTest/missing-dim.conf");
      fail("expected IllegalArgumentException for vector column without dim");
    } catch (Exception e) {
      // either wrapped or direct; assert message reaches us
      Throwable t = e;
      while (t != null && !(t instanceof IllegalArgumentException)) {
        t = t.getCause();
      }
      assertTrue("expected IllegalArgumentException in cause chain: " + e,
          t instanceof IllegalArgumentException);
      assertTrue(t.getMessage().contains("dim"));
    }
  }

  @Test
  public void testUnsafeIdentifierRejected() {
    try {
      buildBypass("PostgresIndexerTest/unsafe-ident.conf");
      fail("expected IllegalArgumentException for unsafe identifier");
    } catch (Exception e) {
      Throwable t = e;
      while (t != null && !(t instanceof IllegalArgumentException)) {
        t = t.getCause();
      }
      assertTrue("expected IllegalArgumentException: " + e, t instanceof IllegalArgumentException);
    }
  }
}
