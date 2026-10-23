package com.kmwllc.lucille.connector.jdbc;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.TimeZone;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DatabaseConnectorTest {

  private static final Logger log = LoggerFactory.getLogger(DatabaseConnectorTest.class);

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("db-test-start.sql");
  private static final TimeZone originalTimeZone = TimeZone.getDefault();

  private Publisher publisher;
  private TestMessenger messenger;

  private String testRunId = "testRunId";
  private String connectorName = "testConnector";
  private String pipelineName = "testPipeline";

  @Before
  public void initTestMode() throws Exception {
    // set com.kmwllc.lucille into loopback mode for local / standalone testing.
    messenger = new TestMessenger();
    publisher = new PublisherImpl(ConfigFactory.empty(), messenger, testRunId, pipelineName);
  }

  @BeforeClass
  public static void setUpTimeZone() {
    // setting timezone for date/timestamp type insertion and retrieval from dbHelper
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterClass
  public static void resetTimeZone() throws Exception {
    TimeZone.setDefault(originalTimeZone);
  }

  @Test
  public void testDatabaseConnectorMixed() throws Exception {
    // Create the test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,int_field,bool_field from mixed order by id");
    configValues.put("idField", "id");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);

    // start the connector
    connector.execute(publisher);

    // Confirm there were 3 results.
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(2, docsSentForProcessing.size());

    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals((Integer)3, docsSentForProcessing.get(0).getInt("int_field"));
    assertEquals(true, docsSentForProcessing.get(0).getBoolean("bool_field"));

    assertEquals("2", docsSentForProcessing.get(1).getId());
    assertEquals((Integer)4, docsSentForProcessing.get(1).getInt("int_field"));
    assertEquals(false, docsSentForProcessing.get(1).getBoolean("bool_field"));

    connector.close();
  }

  @Test
  public void testDatabaseConnector() throws Exception {
    // Create the test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name,type from animal order by id");
    configValues.put("idField", "id");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);

    // start the connector
    connector.execute(publisher);

    // Confirm there were 3 results.
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(3, docsSentForProcessing.size());

    // System.out.println(docsSentForProcessing.get(0));
    // confirm first doc is 1
    assertEquals("1", docsSentForProcessing.get(0).getId());
    assertEquals("Matt", docsSentForProcessing.get(0).getStringList("name").get(0));
    assertEquals("Human", docsSentForProcessing.get(0).getStringList("type").get(0));

    assertEquals("2", docsSentForProcessing.get(1).getId());
    assertEquals("Sonny", docsSentForProcessing.get(1).getStringList("name").get(0));
    assertEquals("Cat", docsSentForProcessing.get(1).getStringList("type").get(0));

    assertEquals("3", docsSentForProcessing.get(2).getId());
    assertEquals("Blaze", docsSentForProcessing.get(2).getStringList("name").get(0));
    assertEquals("Cat", docsSentForProcessing.get(2).getStringList("type").get(0));

    connector.close();
  }

  @Test
  public void testDatabaseConnectionRetry() throws Exception {
    // Create the test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "lousy connection String"); // lousy connection string to test retry
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,int_field,bool_field from mixed order by id");
    configValues.put("idField", "id");
    configValues.put("connectionRetries", 1);
    // put sleep to 1 millisecond to avoid too much time for testing
    configValues.put("connectionRetryPause", 1);

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // creating response after getConnection is called
    // we are retrying once, so if we throw exception twice, we should get a ConnectorException
    Connection mockConnection = mock(Connection.class);
    when(mockConnection.createStatement()).thenReturn(mock(Statement.class));
    Answer<Connection> exceptionAnswer = new Answer<>() {
      private int count = 0;
      @Override
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        if (count++ < 2) {  // throw twice to fail overall connection
          throw new SQLException("Connection failed");
        }
        return mockConnection;
      }
    };

    try (MockedStatic<DriverManager> mockDriverManager = mockStatic(DriverManager.class)) {
      mockDriverManager.when(() -> DriverManager.getConnection("lousy connection String", "", ""))
          .thenAnswer(exceptionAnswer);

      DatabaseConnector connector = new DatabaseConnector(config);

      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
      // verify that getConnection was called twice --first attempt and retry attempt
      mockDriverManager.verify(
          () -> DriverManager.getConnection(eq("lousy connection String"), eq(""), eq("")), times(2));
      connector.close();
    }
    // test that we did not get connection, and so connection.createStatement would not be called
    verify(mockConnection, times(0)).createStatement(anyInt(), anyInt());
  }


  @Test
  public void testDatabaseConnectionRetryAndConnect() throws Exception {
    // Create the test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "lousy connection String"); // lousy connection string to test retry
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,int_field,bool_field from mixed order by id");
    configValues.put("idField", "id");
    configValues.put("connectionRetries", 1);
    // put sleep to 1 millisecond to avoid too much time for testing
    configValues.put("connectionRetryPause", 1);

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // creating response after getConnection is called
    Connection mockConnection = mock(Connection.class);
    when(mockConnection.createStatement()).thenReturn(mock(Statement.class));
    // we are retrying once and expecting back a Connection
    Answer<Connection> exceptionAnswer = new Answer<>() {
      private int count = 0;
      @Override
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        if (count++ == 0) {  // throw exception once
          throw new SQLException("Connection failed");
        }
        return mockConnection;
      }
    };

    try (MockedStatic<DriverManager> mockDriverManager = mockStatic(DriverManager.class)) {
      mockDriverManager.when(() -> DriverManager.getConnection("lousy connection String", "", ""))
          .thenAnswer(exceptionAnswer);

      DatabaseConnector connector = new DatabaseConnector(config);

      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
      // verify that getConnection was called twice --first attempt and retry attempt
      mockDriverManager.verify(
          () -> DriverManager.getConnection(eq("lousy connection String"), eq(""), eq("")), times(2));
      connector.close();
    }
    // check that we have proceeded outside of getting connection as connection has been established
    verify(mockConnection, times(1)).createStatement(anyInt(), anyInt());
  }

  @Test
  public void testCompaniesQuery() throws ConnectorException, SQLException {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select company_id, name from companies order by company_id");
    configValues.put("idField", "company_id");
    configValues.put("docIdPrefix", "company-");

    Config config = ConfigFactory.parseMap(configValues);

    DatabaseConnector connector = new DatabaseConnector(config);

    connector.execute(publisher);

    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(2, docsSentForProcessing.size());

    // The doc ID should have the 'company-' prefix
    assertEquals("company-1-1", docsSentForProcessing.get(0).getId());
    // There should also be a company_id field containing the company ID
    assertEquals("1-1", docsSentForProcessing.get(0).getStringList("company_id").get(0));
    assertEquals("Acme", docsSentForProcessing.get(0).getStringList("name").get(0));

    assertEquals("company-1-2", docsSentForProcessing.get(1).getId());
    assertEquals("1-2", docsSentForProcessing.get(1).getStringList("company_id").get(0));
    // The name field shouldn't be set because the value was null in the database
    assertFalse(docsSentForProcessing.get(1).has("name"));

    connector.close();
  }

  @Test
  public void testRetrievingJDBCTypes() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from test_data_types");
    configValues.put("idField", "id");

    Config config = ConfigFactory.parseMap(configValues);

    DatabaseConnector connector = new DatabaseConnector(config);
    connector.execute(publisher);
    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(2, docsSentForProcessing.size());

    Document d1 = docsSentForProcessing.get(0);
    Document d2 = docsSentForProcessing.get(1);

    // String
    assertEquals("Test VARCHAR", d1.getString("varchar_col"));
    assertEquals("CHAR Test ", d1.getString("char_col")); //char_col is storing fixed number of characters
    assertEquals("Long VARCHAR test data", d1.getString("longvarchar_col"));
    assertEquals("\uD83D\uDE00        ", d1.getString("nchar_col")); // nchar_col is storing fixed number of characters
    assertEquals("こんにちは、世界！", d1.getString("nvarchar_col"));
    assertEquals("こんにちは、世界！長いテキストのテストです。", d1.getString("longnvarchar_col"));
    assertEquals("test clob", d1.getString("clob_col"));
    assertEquals("test nclob", d1.getString("nclob_col"));
    // Integer
    assertEquals(Integer.valueOf(127), d1.getInt("tinyint_col"));
    assertEquals(Integer.valueOf(32767), d1.getInt("smallint_col"));
    assertEquals(Integer.valueOf(2147483647), d1.getInt("integer_col"));
    // Long
    assertEquals(Long.valueOf("9223372036854775807"), d1.getLong("bigint_col"));
    // Double
    assertEquals(Double.valueOf(3.14159265359), d1.getDouble("double_col"));
    assertEquals(Double.valueOf(9877.0), d1.getDouble("decimal_col"));
    assertEquals(Double.valueOf(1.0), d1.getDouble("numeric_col"));
    // Float
    assertEquals(Float.valueOf("2.71828"), d1.getFloat("float_col"));
    assertEquals(Float.valueOf("1.414214"), d1.getFloat("real_col"));
    // Boolean
    assertEquals(true, d1.getBoolean("boolean_col"));
    assertEquals(true, d1.getBoolean("bit_col"));
    // Date & Timestamp
    assertEquals(Date.valueOf("2024-07-30"), d1.getDate("date_col"));
    assertEquals(Timestamp.valueOf("1970-01-01 00:00:01.0"), d1.getTimestamp("timestamp_col"));

    // Null (would not be added)
    assertFalse(d1.has("nullable_int"));
    assertNull(d1.getString("nullable_varchar"));
    assertFalse(d1.has("nullable_date")); // date would not get added to docs if null

    // byte[]
    assertEquals("This is a test blob.", new String(d1.getBytes("blob_col"), StandardCharsets.UTF_8));
    byte[] binaryColBytes = d1.getBytes("binary_col");
    byte[] expectedBytes = new byte[] {(byte)0xBE, (byte)0xEF, (byte)0xBE, (byte)0xEF};
    assertArrayEquals(expectedBytes, Arrays.copyOf(binaryColBytes, 4)); // converting binaryColBytes from array of 100 to 4 and comparing

    byte[] varbinaryColBytes = d1.getBytes("varbinary_col");
    byte[] expectedVarbinaryBytes = new byte[] {
        0x01, 0x23, 0x45, 0x67, (byte)0x89, (byte)0xAB, (byte)0xCD, (byte)0xEF
    };
    assertArrayEquals(expectedVarbinaryBytes, varbinaryColBytes);

    byte[] longVarbinaryColBytes = d1.getBytes("longvarbinary_col");
    byte[] expectedLongVarbinaryBytes = new byte[] {
        0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, (byte)0x88, (byte)0x99, 0x00
    };
    assertArrayEquals(expectedLongVarbinaryBytes, longVarbinaryColBytes);

    // String
    assertEquals("Another", d2.getString("varchar_col"));
    assertEquals("Test      ", d2.getString("char_col")); //char_col is storing fixed number of characters
    assertEquals("More long text here", d2.getString("longvarchar_col"));
    assertEquals("\uD83D\uDE00        ", d2.getString("nchar_col")); // nchar_col is storing fixed number of characters
    assertEquals("안녕하세요, 세계!", d2.getString("nvarchar_col"));
    assertEquals("안녕하세요, 세계! 긴 텍스트 테스트입니다.", d2.getString("longnvarchar_col"));
    assertEquals("test clob", d2.getString("clob_col"));
    assertEquals("test nclob", d2.getString("nclob_col"));
    // Integer
    assertEquals(Integer.valueOf(-128), d2.getInt("tinyint_col"));
    assertEquals(Integer.valueOf(-32768), d2.getInt("smallint_col"));
    assertEquals(Integer.valueOf(-2147483648), d2.getInt("integer_col"));
    // Long
    assertEquals(Long.valueOf("-9223372036854775808"), d2.getLong("bigint_col"));
    // Double
    assertEquals(Double.valueOf(1.41421356237), d2.getDouble("double_col"));
    assertEquals(Double.valueOf(500.0), d2.getDouble("decimal_col"));
    assertEquals(Double.valueOf(100000.0), d2.getDouble("numeric_col"));
    // Float
    assertEquals(Float.valueOf("1.61803"), d2.getFloat("float_col"));
    assertEquals(Float.valueOf("3.141592"), d2.getFloat("real_col"));
    // Boolean
    assertEquals(false, d2.getBoolean("boolean_col"));
    assertEquals(false, d2.getBoolean("bit_col"));
    // Date & Timestamp
    assertEquals(Date.valueOf("2023-01-01"), d2.getDate("date_col"));
    assertEquals(Timestamp.valueOf("2038-01-19 03:14:07.0"), d2.getTimestamp("timestamp_col"));
    // Null (Would not be added to document)
    assertFalse(d2.has("nullable_int"));
    assertNull(d2.getString("nullable_varchar"));
    assertFalse(d2.has("nullable_date")); // date would not get added to docs if null
    // byte[]
    assertEquals("This is a test blob2.", new String(d2.getBytes("blob_col"), StandardCharsets.UTF_8));

    binaryColBytes = d2.getBytes("binary_col");
    expectedBytes = new byte[] {(byte)0xBE, (byte)0xEF, (byte)0xBE, (byte)0xEF};
    assertArrayEquals(expectedBytes, Arrays.copyOf(binaryColBytes, 4)); // converting binaryColBytes from array of 100 to 4 and comparing

    varbinaryColBytes = d2.getBytes("varbinary_col");
    expectedVarbinaryBytes = new byte[] {
        (byte)0xFE, (byte)0xDC, (byte)0xBA, (byte)0x98,
        0x76, 0x54, 0x32, 0x10
    };
    assertArrayEquals(expectedVarbinaryBytes, varbinaryColBytes);

    longVarbinaryColBytes = d2.getBytes("longvarbinary_col");
    expectedLongVarbinaryBytes = new byte[] {
        (byte)0xAA, (byte)0xBB, (byte)0xCC, (byte)0xDD, (byte)0xEE, (byte)0xFF,
        0x00, 0x11, 0x22, 0x33
    };
    assertArrayEquals(expectedLongVarbinaryBytes, longVarbinaryColBytes);

    connector.close();
  }

  @Test
  public void testJoiningDatabaseConnector() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);

    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name from animal");
    configValues.put("idField", "id");
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as meal_id, animal_id,name from meal order by animal_id");
    // The join fields. id goes to animal_id
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("animal_id");
    configValues.put("otherSQLs", otherSql);
    configValues.put("otherJoinFields", otherJoinFields);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // run the connector
    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    // TODO: better verification / edge cases.. also formalize the "children" docs.
    String expected = "{\"id\":\"1\",\"name\":\"Matt\",\"___children\":[{\"id\":\"0\",\"meal_id\":1,\"animal_id\":1,\"name\":\"breakfast\"},{\"id\":\"1\",\"meal_id\":2,\"animal_id\":1,\"name\":\"lunch\"},{\"id\":\"2\",\"meal_id\":3,\"animal_id\":1,\"name\":\"dinner\"}],\"run_id\":\"testRunId\"}";
    assertEquals(expected, docs.get(0).toString());

    connector.close();
  }

  /**
   * Join scenario where the primary (parent) result set has a gap relative to the child result set:
   * the child query returns a join key that the primary query does not.
   *
   * <p>Parents are animals 1 and 3 (animal 2 is filtered out, creating the gap). The child (meal) query
   * returns a single orphan row for animal_id 2 sitting between animal_id 1 and the animal_id 3 rows, so
   * the child join-key sequence is: 1, 2, 3, 3. Parent 1 matches the animal_id=1 row; parent 3 matches
   * both animal_id=3 rows (meal ids 6 and 7). The orphan animal_id=2 row belongs to no parent and is
   * simply skipped.
   */
  @Test
  public void testJoiningDatabaseConnectorGapBetweenParents() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    // Primary parents are animals 1 and 3 - animal 2 is intentionally excluded, creating a gap
    // relative to the child result set below.
    configValues.put("sql", "select id,name from animal where id in (1,3) order by id");
    configValues.put("idField", "id");
    // Child result set with a single orphan (animal_id=2) between the parent keys.
    // meal id 1 -> animal_id 1, meal id 4 -> animal_id 2 (orphan), meal ids 6,7 -> animal_id 3.
    // Ordered by animal_id, the child join-key sequence is: 1, 2, 3, 3.
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as meal_id, animal_id, name from meal where id in (1,4,6,7) order by animal_id");
    configValues.put("otherSQLs", otherSql);
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("animal_id");
    configValues.put("otherJoinFields", otherJoinFields);

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);
    try {
      connector.execute(publisher);

      List<Document> docs = messenger.getDocsSentForProcessing();
      assertEquals(2, docs.size());

      // Parent 1 (animal id 1) should have exactly one child: meal id 1 (breakfast).
      Document parent1 = docs.get(0);
      assertEquals("1", parent1.getId());
      assertEquals(1, parent1.getChildren().size());
      assertEquals((Integer) 1, parent1.getChildren().get(0).getInt("meal_id"));

      // Parent 3 (animal id 3) should have BOTH of its matching children: meal ids 6 and 7.
      Document parent3 = docs.get(1);
      assertEquals("3", parent3.getId());
      assertEquals(2, parent3.getChildren().size());

      List<Integer> childMealIds = new ArrayList<>();
      for (Document child : parent3.getChildren()) {
        childMealIds.add(child.getInt("meal_id"));
      }
      assertTrue("expected meal id 6 to be present as a child of parent 3", childMealIds.contains(6));
      assertTrue("expected meal id 7 to be present as a child of parent 3", childMealIds.contains(7));
    } finally {
      connector.close();
    }
  }

  /**
   * Builds a joining-connector config against the seeded animal/meal tables with the given primary and
   * child queries, joining on animal_id.
   */
  private Config joinConfig(String sql, String otherSql) {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", sql);
    configValues.put("idField", "id");
    ArrayList<String> otherSqls = new ArrayList<>();
    otherSqls.add(otherSql);
    configValues.put("otherSQLs", otherSqls);
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("animal_id");
    configValues.put("otherJoinFields", otherJoinFields);
    return ConfigFactory.parseMap(configValues);
  }

  /** Runs a joining connector built by {@link #joinConfig} and returns the published documents. */
  private List<Document> runJoin(String sql, String otherSql) throws Exception {
    DatabaseConnector connector = new DatabaseConnector(joinConfig(sql, otherSql));
    try {
      connector.execute(publisher);
    } finally {
      connector.close();
    }
    return messenger.getDocsSentForProcessing();
  }

  private static List<Integer> mealIds(Document parent) {
    List<Integer> ids = new ArrayList<>();
    for (Document child : parent.getChildren()) {
      ids.add(child.getInt("meal_id"));
    }
    return ids;
  }

  /**
   * Two consecutive orphan child rows between two parents. The sorted merge consumes rows in pairs when
   * it skips a "too small" key, so an even-length run of orphans happens to lose nothing even on the
   * buggy implementation; this case exists so that a rewrite of the merge is checked for both the odd
   * (see testJoiningDatabaseConnectorGapBetweenParents) and the even case.
   *
   * <p>Parents are animals 1 and 3; children are meals 1 (animal 1), 4 and 5 (animal 2, both orphans),
   * 6 and 7 (animal 3). Child join-key sequence: 1, 2, 2, 3, 3.
   */
  @Test
  public void testJoiningDatabaseConnectorTwoConsecutiveOrphans() throws Exception {
    List<Document> docs = runJoin(
        "select id,name from animal where id in (1,3) order by id",
        "select id as meal_id, animal_id, name from meal where id in (1,4,5,6,7) order by animal_id, id");

    assertEquals(2, docs.size());
    assertEquals("1", docs.get(0).getId());
    assertEquals(Arrays.asList(1), mealIds(docs.get(0)));
    assertEquals("3", docs.get(1).getId());
    assertEquals(Arrays.asList(6, 7), mealIds(docs.get(1)));
  }

  /**
   * Orphan child rows before the first parent and after the last parent. The only parent is animal 2;
   * children are meals 1, 2, 3 (animal 1, orphans before), 4, 5 (animal 2), 6, 7 (animal 3, orphans
   * after). Child join-key sequence: 1, 1, 1, 2, 2, 3, 3. Animal 2 must receive both of its meals; the
   * trailing orphans must be ignored without error.
   */
  @Test
  public void testJoiningDatabaseConnectorOrphansBeforeFirstAndAfterLastParent() throws Exception {
    List<Document> docs = runJoin(
        "select id,name from animal where id = 2 order by id",
        "select id as meal_id, animal_id, name from meal order by animal_id, id");

    assertEquals(1, docs.size());
    assertEquals("2", docs.get(0).getId());
    assertEquals(Arrays.asList(4, 5), mealIds(docs.get(0)));
  }

  /**
   * The child result set is exhausted before the last parents are reached: all three animals are parents
   * but only animal 1 has child rows. Animal 1 gets its three meals; animals 2 and 3 are published with no
   * children and the run completes. This exercises the "cursor already past the end" path separately from
   * the empty-result-set path in testJoiningDatabaseConnectorEmptyChildResultSet.
   */
  @Test
  public void testJoiningDatabaseConnectorChildResultSetExhaustedBeforeLastParent() throws Exception {
    List<Document> docs = runJoin(
        "select id,name from animal order by id",
        "select id as meal_id, animal_id, name from meal where animal_id = 1 order by animal_id, id");

    assertEquals(3, docs.size());
    assertEquals("1", docs.get(0).getId());
    assertEquals(Arrays.asList(1, 2, 3), mealIds(docs.get(0)));
    assertFalse("animal 2 has no meals in the child query", docs.get(1).hasChildren());
    assertFalse("animal 3 has no meals in the child query", docs.get(2).hasChildren());
  }

  /**
   * Join scenario where an otherSQL (child) query returns no rows at all - for example, an empty child
   * table or a child filter that matches nothing. Each parent should simply be published with no children,
   * and the overall run should complete successfully.
   */
  @Test
  public void testJoiningDatabaseConnectorEmptyChildResultSet() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name from animal order by id");
    configValues.put("idField", "id");
    // Child query that is guaranteed to return zero rows.
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as meal_id, animal_id, name from meal where 1=0 order by animal_id");
    configValues.put("otherSQLs", otherSql);
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("animal_id");
    configValues.put("otherJoinFields", otherJoinFields);

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);
    try {
      // An empty child result set must not abort the run.
      connector.execute(publisher);

      List<Document> docs = messenger.getDocsSentForProcessing();
      // All three parents should still be published...
      assertEquals(3, docs.size());
      // ...each with no children, since the child query returned nothing.
      for (Document doc : docs) {
        assertFalse("parent " + doc.getId() + " should have no children", doc.hasChildren());
      }
    } finally {
      connector.close();
    }
  }

  @Test
  public void testJoiningDatabaseConnectorStringType() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select name,type from animal order by name");
    configValues.put("idField", "name");
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as network_id,name,friends_with from network order by name");
    configValues.put("otherSQLs", otherSql);
    // The join fields. id goes to animal_id
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("name");
    configValues.put("otherJoinFields", otherJoinFields);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // run the connector
    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());
    String expected = "{\"id\":\"Matt\",\"name\":\"Matt\",\"type\":\"Human\",\"___children\":[{\"id\":\"0\",\"network_id\":3,\"name\":\"Matt\",\"friends_with\":\"Bob\"},{\"id\":\"1\",\"network_id\":4,\"name\":\"Matt\",\"friends_with\":\"Sonny\"},{\"id\":\"2\",\"network_id\":5,\"name\":\"Matt\",\"friends_with\":\"Blaze\"}],\"run_id\":\"testRunId\"}";
    assertEquals(expected, docs.get(1).toString());

    connector.close();
  }

  @Test
  public void testJoiningDatabaseConnectorNonComparable() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select name,type from animal order by name");
    configValues.put("idField", "name");
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as _id,name,metadata from nonComparable order by name");
    configValues.put("otherSQLs", otherSql);
    // The join fields. id goes to animal_id
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("metadata");
    configValues.put("otherJoinFields", otherJoinFields);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // throws error as JSON type is not comparable
    assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    connector.close();
  }

  @Test
  public void testJoiningDatabaseConnectorDateType() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select name,type,birthday from animal where id=2");
    configValues.put("idField", "birthday");
    // a list of other sql statements
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as adoption_id,name,adopted_on from adopted order by adopted_on");
    configValues.put("otherSQLs", otherSql);
    // The join fields. id goes to animal_id
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("adopted_on");
    configValues.put("otherJoinFields", otherJoinFields);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // run the connector
    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(1, docs.size());
    // birthday is idField
    String expected = "{\"id\":\"2024-07-30\",\"name\":\"Sonny\",\"type\":\"Cat\",\"birthday\":\"2024-07-30T00:00:00Z\",\"___children\":[{\"id\":\"0\",\"adoption_id\":1,\"name\":\"Sonny\",\"adopted_on\":\"2024-07-30T00:00:00Z\"},{\"id\":\"1\",\"adoption_id\":2,\"name\":\"Blaze\",\"adopted_on\":\"2024-07-30T00:00:00Z\"}],\"run_id\":\"testRunId\"}";
    assertEquals(expected, docs.get(0).toString());

    String expectedDateStr = "2024-07-30";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    java.util.Date expectedDate = sdf.parse(expectedDateStr); // this would give a date based on current timezone

    // check that parent and children have the same date value used for joining
    assertEquals(expectedDate, docs.get(0).getDate("birthday"));
    assertEquals(expectedDate, docs.get(0).getChildren().get(0).getDate("adopted_on"));
    assertEquals(expectedDate, docs.get(0).getChildren().get(1).getDate("adopted_on"));

    connector.close();
  }

  // TODO: not implemented yet.
  // @Test
  public void testCollapsingDatabaseConnector() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select animal_id,id,name from meal order by animal_id asc");
    configValues.put("idField", "animal_id");
    configValues.put("collapse", true);
    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);
    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // create a publisher to record all the docs sent to it.
    // run the connector

    connector.execute(publisher);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    for (Document d : docs) {
      System.err.println(d);
    }

    // TODO:
    //    for (Document doc : publisher.getPublishedDocs()) {
    //      System.out.println(doc);
    //    }
    //    // TODO?
    //    assertEquals(3, publisher.getPublishedDocs().size());
    // TODO: more validations.

    connector.close();
  }

  @Test
  public void testClose() throws ConnectorException, SQLException {
    // Create a test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name,type from animal order by id");
    configValues.put("idField", "id");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // call the execute method, then close the connection
    connector.execute(publisher);
    assertEquals(2, dbHelper.countConnections());

    assertFalse(connector.isClosed());
    connector.close();
    // verify that the connection is actually closed
    assertTrue(connector.isClosed());
  }

  @Test
  public void testIdColumnException() throws ConnectorException {
    // Create a test config
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from companies");
    configValues.put("idField", "NONEXISTENT_ID_COLUMN");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // call the execute method, then close the connection
    Throwable exception = assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    assertEquals("Unable to find id column: NONEXISTENT_ID_COLUMN", exception.getCause().getMessage());
    connector.close();
  }

  @Test
  public void testDriverNotFound() throws ConnectorException {
    // Create a test config with a driver class that isn't on the classpath
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "com.nonexistent.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from companies");
    configValues.put("idField", "id");

    // create a config object off that map
    Config config = ConfigFactory.parseMap(configValues);

    // create the connector with the config
    DatabaseConnector connector = new DatabaseConnector(config);
    // the driver can't be loaded, so execute should fail rather than proceeding without a connection
    Throwable exception = assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    assertEquals(ClassNotFoundException.class, exception.getCause().getClass());
    connector.close();
  }

  @Test
  public void testReservedFieldError() throws ConnectorException, SQLException {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select * from table_with_id_column");
    configValues.put("idField", "other_id");

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);

    Throwable exception = assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    assertEquals("Field name \"id\" is reserved, please rename it or add it to the ignore list",
        exception.getCause().getMessage());

    connector.close();
  }

  @Test
  public void testTableWithIdColumn() throws ConnectorException, SQLException {

    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("ignoreColumns", List.of("id"));
    configValues.put("sql", "select id as table_id, * from table_with_id_column");
    configValues.put("idField", "other_id");

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);

    connector.execute(publisher);

    List<Document> docsSentForProcessing = messenger.getDocsSentForProcessing();
    assertEquals(2, docsSentForProcessing.size());

    Document doc1 = docsSentForProcessing.get(0);
    Document doc2 = docsSentForProcessing.get(1);

    // document id is coming from the "other_id" column
    assertEquals("id1", doc1.getId());
    assertEquals("id2", doc2.getId());

    // "id" column is renamed to "table_id"
    assertEquals("1", doc1.getString("table_id"));
    assertEquals("2", doc2.getString("table_id"));

    // "other_id" is still in the document
    assertTrue(doc1.has("other_id"));
    assertEquals("id1", doc1.getString("other_id"));
    assertTrue(doc2.has("other_id"));
    assertEquals("id2", doc2.getString("other_id"));

    connector.close();
  }

  /**
   * Comprehensive type-handling contract for the DatabaseConnector, asserted identically on the primary
   * (parent) path and the otherSQL (child) path. The connector should treat column types the same
   * whichever query they appear in, and it should take a strict stance: supported types are carried onto
   * the document with a faithful value, and any unsupported type fails the run with a ConnectorException
   * (never a silent drop and never a silently converted/corrupted value).
   *
   * <p><b>Supported types</b> (verified on both paths): CHAR/VARCHAR family (String), TINYINT/SMALLINT/
   * INTEGER (Integer), BIGINT (Long), DOUBLE and ordinary DECIMAL (Double), REAL/FLOAT (Float),
   * BOOLEAN/BIT (Boolean), DATE/TIMESTAMP (stored as ISO-8601 instant strings, read back via
   * getDate/getTimestamp), and the binary family (byte[]). Plain TIME has no time zone and round-trips
   * losslessly as a string, so it is also treated as supported here.
   *
   * <p><b>Unsupported types</b> (must throw on both paths): reading zone-aware temporal types through
   * getTimestamp normalizes away the offset and corrupts the value, and structural/vendor types have no
   * safe scalar mapping, so under the strict stance both must fail the run rather than be silently
   * dropped, reinterpreted, or converted.
   *
   * <p>Every unsupported type that H2 (in-memory) can actually represent is exercised below via
   * {@link #assertUnsupportedTypeThrowsOnBothPaths}: TIME WITH TIME ZONE, TIMESTAMP WITH TIME ZONE, UUID,
   * JSON, GEOMETRY, ARRAY, INTERVAL, and ENUM. (Note H2 maps several of these onto surprising JDBC types -
   * e.g. UUID -> Types.BINARY, JSON/GEOMETRY/INTERVAL/ENUM -> Types.OTHER - which is exactly why the
   * current code handles them inconsistently; the strict contract is that all of them throw.) Types that
   * H2 cannot represent in-memory and are therefore NOT exercised here, but to which the same must-throw
   * contract applies against a database that supports them: STRUCT, SQLXML, ROWID, REF, DATALINK.
   *
   * <p>NOTE: ordinary DECIMAL/NUMERIC is supported-as-Double here; the separate concern that high-precision
   * DECIMAL is read lossily via getDouble is tracked elsewhere and intentionally NOT asserted by this test.
   *
   * <p>The tests are resolution-agnostic about the exact exception type: they assert at the connector
   * boundary that a ConnectorException is raised for unsupported types.
   *
   * <p>This method covers the supported types on both paths; the unsupported types each have their own
   * test method below (testUnsupportedType*FailsTheRun) so that every type's outcome is reported
   * independently rather than hidden behind the first failure.
   */
  @Test
  public void testSupportedTypesHandledIdenticallyOnParentAndChildPaths() throws Exception {
    // A parent table and a child table carrying the same set of supported column types. The child joins
    // to the parent on parent_id so every supported type flows through the otherSQL path as well.
    String setup =
        "CREATE TABLE typed_parent("
            + "id INT, str_col VARCHAR(20), int_col INTEGER, long_col BIGINT, dbl_col DOUBLE, "
            + "dec_col DECIMAL(10,2), flt_col REAL, bool_col BOOLEAN, date_col DATE, ts_col TIMESTAMP, "
            + "bin_col VARBINARY(8), time_col TIME); "
            + "INSERT INTO typed_parent VALUES "
            + "(1, 'hello', 42, 9000000000, 3.5, 12.50, 1.25, TRUE, DATE '2024-07-30', "
            + "TIMESTAMP '2024-07-30 00:00:01', X'0A0B', TIME '10:15:30'); "
            + "CREATE TABLE typed_child("
            + "id INT, parent_id INT, c_str VARCHAR(20), c_int INTEGER, c_long BIGINT, c_dbl DOUBLE, "
            + "c_dec DECIMAL(10,2), c_flt REAL, c_bool BOOLEAN, c_date DATE, c_ts TIMESTAMP, "
            + "c_bin VARBINARY(8), c_time TIME); "
            + "INSERT INTO typed_child VALUES "
            + "(1, 1, 'world', 7, 8000000000, 2.5, 99.99, 4.75, FALSE, DATE '2023-01-01', "
            + "TIMESTAMP '2023-01-01 00:00:02', X'0C0D', TIME '12:00:00');";

    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("preSQL", setup);
    configValues.put("sql",
        "select id, str_col, int_col, long_col, dbl_col, dec_col, flt_col, bool_col, date_col, ts_col, "
            + "bin_col, time_col from typed_parent order by id");
    configValues.put("idField", "id");
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as child_id, parent_id, c_str, c_int, c_long, c_dbl, c_dec, c_flt, c_bool, "
        + "c_date, c_ts, c_bin, c_time from typed_child order by parent_id");
    configValues.put("otherSQLs", otherSql);
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("parent_id");
    configValues.put("otherJoinFields", otherJoinFields);

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);
    try {
      connector.execute(publisher);

      List<Document> docs = messenger.getDocsSentForProcessing();
      assertEquals(1, docs.size());

      Document parent = docs.get(0);
      assertEquals(1, parent.getChildren().size());
      Document child = parent.getChildren().get(0);

      // --- Supported types on the PRIMARY path ---
      assertEquals("hello", parent.getString("str_col"));
      assertEquals((Integer) 42, parent.getInt("int_col"));
      assertEquals(Long.valueOf(9000000000L), parent.getLong("long_col"));
      assertEquals(Double.valueOf(3.5), parent.getDouble("dbl_col"));
      // ordinary DECIMAL round-trips as a Double (high-precision loss tracked separately, not asserted here)
      assertEquals(Double.valueOf(12.50), parent.getDouble("dec_col"));
      assertEquals(Float.valueOf(1.25f), parent.getFloat("flt_col"));
      assertEquals(Boolean.TRUE, parent.getBoolean("bool_col"));
      assertEquals(Date.valueOf("2024-07-30"), parent.getDate("date_col"));
      assertEquals(Timestamp.valueOf("2024-07-30 00:00:01.0"), parent.getTimestamp("ts_col"));
      assertArrayEquals(new byte[] {0x0A, 0x0B}, parent.getBytes("bin_col"));
      // plain TIME is lossless as a string and must not be silently dropped
      assertTrue("TIME column must be present on the parent document", parent.has("time_col"));

      // --- The SAME supported types on the CHILD path ---
      assertEquals("world", child.getString("c_str"));
      assertEquals((Integer) 7, child.getInt("c_int"));
      assertEquals(Long.valueOf(8000000000L), child.getLong("c_long"));
      assertEquals(Double.valueOf(2.5), child.getDouble("c_dbl"));
      assertEquals(Double.valueOf(99.99), child.getDouble("c_dec"));
      assertEquals(Float.valueOf(4.75f), child.getFloat("c_flt"));
      assertEquals(Boolean.FALSE, child.getBoolean("c_bool"));
      assertEquals(Date.valueOf("2023-01-01"), child.getDate("c_date"));
      assertEquals(Timestamp.valueOf("2023-01-01 00:00:02.0"), child.getTimestamp("c_ts"));
      assertArrayEquals(new byte[] {0x0C, 0x0D}, child.getBytes("c_bin"));
      assertTrue("TIME column must be present on the child document", child.has("c_time"));
    } finally {
      connector.close();
    }

  }

  // --- Unsupported types must throw a ConnectorException, on BOTH paths ---
  // One test per type so that each type's result is visible independently. Each case creates a scratch
  // table (via preSQL) with an id, an integer join key (animal_id=1 so it joins to seed animal 1), and the
  // unsupported-typed column as a NON-join column - so any thrown exception is attributable to type
  // handling, not to comparing the join keys. Every unsupported type H2 can represent in-memory is covered.

  @Test
  public void testUnsupportedTypeTimeWithTimeZoneFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("TIME WITH TIME ZONE", "TIME '10:15:30-04:00'");
  }

  @Test
  public void testUnsupportedTypeTimestampWithTimeZoneFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP WITH TIME ZONE '2023-01-01 00:00:00-04:00'");
  }

  /**
   * H2 reports UUID as {@code Types.BINARY}, so the JDBC type code alone does not distinguish it from a
   * genuine binary column; the strict contract still requires it to fail rather than be stored as 16 raw
   * bytes, which means the implementation must look at the column type name as well as the code.
   */
  @Test
  public void testUnsupportedTypeUuidFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("UUID", "RANDOM_UUID()");
  }

  @Test
  public void testUnsupportedTypeJsonFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("JSON", "JSON '{\"a\":1}'");
  }

  @Test
  public void testUnsupportedTypeGeometryFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("GEOMETRY", "'POINT(1 1)'");
  }

  @Test
  public void testUnsupportedTypeArrayFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("INTEGER ARRAY", "ARRAY[1,2,3]");
  }

  @Test
  public void testUnsupportedTypeIntervalFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("INTERVAL DAY", "INTERVAL '1' DAY");
  }

  @Test
  public void testUnsupportedTypeEnumFailsTheRun() throws Exception {
    assertUnsupportedTypeThrowsOnBothPaths("ENUM('a','b')", "'a'");
  }

  /**
   * Asserts that a column of the given unsupported SQL type causes the connector to throw a
   * ConnectorException both when the column appears in the primary query and when it appears in an
   * otherSQL (child) query. This encodes the strict stance: an unsupported type is a misconfiguration
   * that must fail the run rather than being silently dropped, reinterpreted, or converted.
   *
   * <p>For each path a fresh scratch table {@code unsupported_type_probe} is created holding an id, an
   * integer {@code join_key} (always 1, to join to seed animal 1), and a single {@code bad_col} of the
   * unsupported type. The column is a non-join column so a thrown exception is attributable to type
   * handling rather than to join-key comparison.
   *
   * @param columnDdl the SQL type as written in a CREATE TABLE column definition (e.g. "UUID",
   *                  "TIME WITH TIME ZONE", "INTEGER ARRAY", "ENUM('a','b')")
   * @param valueSql  a SQL literal/expression producing a value of that type (e.g. "RANDOM_UUID()")
   */
  private void assertUnsupportedTypeThrowsOnBothPaths(String columnDdl, String valueSql) throws Exception {
    String setup =
        "DROP TABLE IF EXISTS unsupported_type_probe; "
            + "CREATE TABLE unsupported_type_probe(id INT, join_key INT, bad_col " + columnDdl + "); "
            + "INSERT INTO unsupported_type_probe VALUES (1, 1, " + valueSql + ");";

    // Primary path: the unsupported column is selected directly by the primary query.
    {
      HashMap<String, Object> cfg = new HashMap<>();
      cfg.put("name", connectorName);
      cfg.put("pipeline", pipelineName);
      cfg.put("driver", "org.h2.Driver");
      cfg.put("connectionString", "jdbc:h2:mem:test");
      cfg.put("jdbcUser", "");
      cfg.put("jdbcPassword", "");
      cfg.put("preSQL", setup);
      cfg.put("sql", "select id, bad_col from unsupported_type_probe order by id");
      cfg.put("idField", "id");

      DatabaseConnector connector = new DatabaseConnector(ConfigFactory.parseMap(cfg));
      try {
        assertThrows(columnDdl + " in the primary query must fail the run",
            ConnectorException.class, () -> connector.execute(publisher));
      } finally {
        connector.close();
      }
    }

    // reset the publisher so child-path docs don't accumulate with the primary-path attempt
    messenger = new TestMessenger();
    publisher = new PublisherImpl(ConfigFactory.empty(), messenger, testRunId, pipelineName);

    // Child path: seed animal 1 joined to an otherSQL that selects the unsupported column.
    {
      HashMap<String, Object> cfg = new HashMap<>();
      cfg.put("name", connectorName);
      cfg.put("pipeline", pipelineName);
      cfg.put("driver", "org.h2.Driver");
      cfg.put("connectionString", "jdbc:h2:mem:test");
      cfg.put("jdbcUser", "");
      cfg.put("jdbcPassword", "");
      cfg.put("preSQL", setup);
      cfg.put("sql", "select id, name from animal where id=1 order by id");
      cfg.put("idField", "id");
      ArrayList<String> otherSql = new ArrayList<>();
      otherSql.add("select id as probe_id, join_key, bad_col from unsupported_type_probe order by join_key");
      cfg.put("otherSQLs", otherSql);
      ArrayList<String> otherJoinFields = new ArrayList<>();
      otherJoinFields.add("join_key");
      cfg.put("otherJoinFields", otherJoinFields);

      DatabaseConnector connector = new DatabaseConnector(ConfigFactory.parseMap(cfg));
      try {
        assertThrows(columnDdl + " in an otherSQL query must fail the run",
            ConnectorException.class, () -> connector.execute(publisher));
      } finally {
        connector.close();
      }
    }
  }

  /**
   * The join is a sorted merge: the primary and child result sets are walked in lockstep by join key, so
   * it is only correct when both are ordered by that key. When a child result set is not ordered by its
   * join key, the merge currently drops matching child rows while the run reports success - silent data
   * loss. This test fails on that behavior. It does not prescribe the resolution: it passes whether the
   * fix is to attach all matching children anyway OR to raise a ConnectorException on out-of-order input.
   * It only fails if the run completes while silently dropping matching children.
   *
   * <p>Child rows for animal_id 1 (meal ids 1,2,3) are returned in descending meal-id order, which places
   * the join keys (animal_id) in descending order relative to how the merge walks the cursor.
   */
  @Test
  public void testJoiningDatabaseConnectorUnorderedChildResultSet() throws Exception {
    HashMap<String, Object> configValues = new HashMap<>();
    configValues.put("name", connectorName);
    configValues.put("pipeline", pipelineName);
    configValues.put("driver", "org.h2.Driver");
    configValues.put("connectionString", "jdbc:h2:mem:test");
    configValues.put("jdbcUser", "");
    configValues.put("jdbcPassword", "");
    configValues.put("sql", "select id,name from animal order by id");
    configValues.put("idField", "id");
    // Child query NOT ordered by the join key (animal_id). Ordering by meal id descending yields the
    // animal_id sequence 3,3,2,2,1,1,1 - i.e. join keys in descending order.
    ArrayList<String> otherSql = new ArrayList<>();
    otherSql.add("select id as meal_id, animal_id, name from meal order by id desc");
    configValues.put("otherSQLs", otherSql);
    ArrayList<String> otherJoinFields = new ArrayList<>();
    otherJoinFields.add("animal_id");
    configValues.put("otherJoinFields", otherJoinFields);

    Config config = ConfigFactory.parseMap(configValues);
    DatabaseConnector connector = new DatabaseConnector(config);
    try {
      try {
        connector.execute(publisher);
      } catch (ConnectorException e) {
        // Loud failure on out-of-order input is an acceptable resolution - no silent data loss.
        return;
      }

      List<Document> docs = messenger.getDocsSentForProcessing();
      assertEquals(3, docs.size());

      // If the run completed, animal 1's three meals (ids 1,2,3) must all be attached as children rather
      // than silently dropped because of the child ordering.
      Document animal1 = docs.get(0);
      assertEquals("1", animal1.getId());
      assertEquals("animal 1's matching children must not be silently dropped for unordered child input",
          3, animal1.getChildren().size());
    } finally {
      connector.close();
    }
  }
}
