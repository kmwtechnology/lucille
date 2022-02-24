package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class QueryDatabaseTest {

  StageFactory factory = StageFactory.of(QueryDatabase.class);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
    "", "db-test-start.sql", "db-test-end.sql");

  @Test
  public void testQueryDatabase() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/config.conf");

    Document d = new Document("id");
    d.setField("input1", "");
    d.setField("fish", "yes");

    stage.processDocument(d);

    assertEquals("", d.getString("output1"));
  }

}
