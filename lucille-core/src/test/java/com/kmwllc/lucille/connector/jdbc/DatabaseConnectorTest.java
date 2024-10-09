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
import java.util.Arrays;
import org.junit.Before;
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
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "",
      "", "db-test-start.sql", "db-test-end.sql");

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

  @Test
  public void testDatabaseConnectorMixed() throws Exception {
    // The only connection to the h2 database should be the dbHelper
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }
  @Test public void testDatabaseConnector() throws Exception {

    // The only connection to the h2 database should be the dbHelper
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testDatabaseConnectionRetry() throws Exception {
    // The only connection to the h2 database should be the dbHelper
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }


  @Test
  public void testDatabaseConnectionRetryAndConnect() throws Exception {
    // The only connection to the h2 database should be the dbHelper
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testCompaniesQuery() throws ConnectorException, SQLException {

    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testRetrievingJDBCTypes() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testJoiningDatabaseConnector() throws Exception {

    assertEquals(1, dbHelper.checkNumConnections());

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
    String expected = "{\"id\":\"1\",\"name\":\"Matt\",\".children\":[{\"id\":\"0\",\"meal_id\":1,\"animal_id\":1,\"name\":\"breakfast\"},{\"id\":\"1\",\"meal_id\":2,\"animal_id\":1,\"name\":\"lunch\"},{\"id\":\"2\",\"meal_id\":3,\"animal_id\":1,\"name\":\"dinner\"}],\"run_id\":\"testRunId\"}";
    assertEquals(expected, docs.get(0).toString());

    connector.close();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  // TODO: not implemented yet.
  // @Test
  public void testCollapsingDatabaseConnector() throws Exception {
    // TODO: implement me
    assertEquals(1, dbHelper.checkNumConnections());

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
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testClose() throws ConnectorException, SQLException {

    assertEquals(1, dbHelper.checkNumConnections());
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
    assertEquals(2, dbHelper.checkNumConnections());

    assertFalse(connector.isClosed());
    connector.close();
    // verify that the connection is actually closed
    assertTrue(connector.isClosed());
    assertEquals(1, dbHelper.checkNumConnections());
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
    assertEquals(1, dbHelper.checkNumConnections());
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
    assertEquals(1, dbHelper.checkNumConnections());
  }
}
