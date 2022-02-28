package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.*;

public class QueryDatabaseTest {

  StageFactory factory = StageFactory.of(QueryDatabase.class);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
    "", "db-test-start.sql", "db-test-end.sql");

  @Test
  public void testSingleKeyField() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/animal.conf");

    Document d = new Document("id");
    d.setField("fish", 3);

    stage.processDocument(d);

    assertEquals("Blaze", d.getString("output1"));
  }

  @Test
  public void testMultivaluedKeyField() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/meal.conf");

    Document d = new Document("id");
    d.setField("fish", 2);
    d.addToField("fish", 1);

    stage.processDocument(d);

    assertEquals("lunch", d.getString("output1"));
  }

  @Test
  public void testMultipleResults() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/data.conf");

    Document d = new Document("id");
    d.setField("input1", "");
    d.setField("fish", 2);

    stage.processDocument(d);

    assertEquals("12", d.getStringList("output1").get(0));
    assertEquals("2", d.getStringList("output2").get(0));
    assertEquals("tiger", d.getStringList("output1").get(1));
    assertEquals("2", d.getStringList("output2").get(1));
  }
}
