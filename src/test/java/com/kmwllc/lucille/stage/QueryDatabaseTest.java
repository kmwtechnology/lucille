package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import org.junit.Rule;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.*;

public class QueryDatabaseTest {

  StageFactory factory = StageFactory.of(QueryDatabase.class);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
    "", "db-test-start.sql", "db-test-end.sql");

  @Test
  public void testSingleKeyField() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/animal.conf");

    Document d = Document.create("id");
    d.setField("name", "Blaze");

    stage.processDocument(d);

    assertEquals("Blaze", d.getString("output1"));
  }

  @Test
  public void testMultivaluedKeyField() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/meal.conf");

    Document d = Document.create("id");
    d.setField("fish", 2);
    d.addToField("fish2", 1);

    stage.processDocument(d);

    assertEquals("lunch", d.getString("output1"));
  }

  @Test
  public void testMultipleResults() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/data.conf");

    Document d = Document.create("id");
    d.setField("fish", 2);

    stage.processDocument(d);

    assertEquals("12", d.getStringList("output1").get(0));
    assertEquals("2", d.getStringList("output2").get(0));
    assertEquals("tiger", d.getStringList("output1").get(1));
    assertEquals("2", d.getStringList("output2").get(1));

    // Integers come out as Integers, not Strings
    assertEquals("{\"id\":\"id\",\"fish\":2,\"output1\":[\"12\",\"tiger\"],\"output2\":[2,2]}", d.toString());
  }

  @Test(expected = StageException.class)
  public void testWrongNumberOfReplacements() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/mismatch.conf");

    Document d = Document.create("id");
    // only one replacement needed, 2 provided
    d.setField("fish", 2);
    d.addToField("fish2", 3);

    stage.processDocument(d);
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/animal.conf");
    assertEquals(
        Set.of(
            "connectionString",
            "inputTypes",
            "driver",
            "jdbcUser",
            "keyFields",
            "name",
            "returnTypes",
            "jdbcPassword",
            "conditions",
            "class",
            "sql"),
        stage.getLegalProperties());
  }
}
