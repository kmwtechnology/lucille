package com.kmwllc.lucille.stage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.TimeZone;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import java.util.Arrays;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedStatic;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Set;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

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
            "jdbcPassword",
            "conditions",
            "class",
            "sql",
            "connectionRetries",
            "connectionRetryPause",
            "conditionPolicy"),
        stage.getLegalProperties());
  }

  @Test
  public void testStop() throws Exception {
    try (MockedStatic<DriverManager> mockedStatic = mockStatic(DriverManager.class)) {
      PreparedStatement statement = mock(PreparedStatement.class);
      Connection connection = mock(Connection.class);
      when(connection.prepareStatement("SELECT name FROM animal WHERE name = ?")).thenReturn(statement);
      mockedStatic.when(() -> DriverManager.getConnection("jdbc:h2:mem:test", "", "")).thenReturn(connection);
      Stage stage = factory.get("QueryDatabaseTest/animal.conf");
      stage.stop();
      verify(connection).close();
      verify(statement).close();
    }
  }

  @Test
  public void testPreparedThrows() throws Exception {
    try (MockedStatic<DriverManager> mockedStatic = mockStatic(DriverManager.class)) {
      PreparedStatement statement = mock(PreparedStatement.class);
      Connection connection = mock(Connection.class);
      when(connection.prepareStatement("SELECT name FROM animal WHERE name = ?")).thenReturn(statement);
      mockedStatic.when(() -> DriverManager.getConnection("jdbc:h2:mem:test", "", "")).thenReturn(connection);
      doThrow(SQLException.class).when(statement).close();
      Stage stage = factory.get("QueryDatabaseTest/animal.conf");
      assertThrows(StageException.class, () -> stage.stop());
    }
  }

  @Test
  public void testStopConnectionThrows() throws Exception {
    try (MockedStatic<DriverManager> mockedStatic = mockStatic(DriverManager.class)) {
      PreparedStatement statement = mock(PreparedStatement.class);
      Connection connection = mock(Connection.class);
      when(connection.prepareStatement("SELECT name FROM animal WHERE name = ?")).thenReturn(statement);
      mockedStatic.when(() -> DriverManager.getConnection("jdbc:h2:mem:test", "", "")).thenReturn(connection);
      doThrow(SQLException.class).when(connection).close();
      Stage stage = factory.get("QueryDatabaseTest/animal.conf");
      assertThrows(StageException.class, () -> stage.stop());
    }
  }

  @Test
  public void testTypes() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/types.conf");

    Document d1 = Document.create("id");
    d1.setField("get_row", 1);
    Document d2 = Document.create("id");
    d2.setField("get_row", 2);
    stage.processDocument(d1);
    stage.processDocument(d2);

    // testing out d1

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
    // Null will not be added to document
    assertFalse(d1.has("nullable_int"));
    assertNull(d1.getString("nullable_varchar"));
    assertFalse(d1.has("nullable_date")); // date would not get added to docs if null
    // byte[] , any above 0x80 has to be cast as byte
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

    // testing out d2

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
    // null will not be added
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
  }

  @Test
  public void testUnsupportedTypeTime() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/time_type.conf");

    Document d = Document.create("id");
    d.setField("get_row", 1);
    // JDBCUtils should log warning
    assertEquals("no Error found", getUnsupportedMsg(stage, d));;
  }

  @Test
  public void testUnsupportedTypeTimeWTimezone() throws StageException {
    Stage stage = factory.get("QueryDatabaseTest/time_w_timezone_type.conf");

    Document d = Document.create("id");
    d.setField("get_row", 1);
    // JDBCUtils should log warning
    assertEquals("no Error found", getUnsupportedMsg(stage, d));
  }

  private String getUnsupportedMsg(Stage stage, Document d) {
    try {
      stage.processDocument(d);
    } catch (StageException e) {
      return e.getMessage();
    }
    return "no Error found";
  }

  @Test
  public void testConnectionRetry() throws StageException, SQLException {
    Connection mockConnection = mock(Connection.class);
    when(mockConnection.prepareStatement(any())).thenReturn(mock(PreparedStatement.class));
    Answer<Connection> exceptionAnswer = new Answer<>() {
      private int count = 0;
      @Override
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        if (count++ < 2) {
          throw new SQLException("Connection failed");
        }
        return mockConnection;
      }
    };

    try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
      mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenAnswer(exceptionAnswer);

      assertThrows(StageException.class, () -> factory.get("QueryDatabaseTest/meal.conf"));
      // verify that getConnection was called twice first attempt and then retry attempt
      mockedDriverManager.verify(() -> DriverManager.getConnection(anyString(), anyString(), anyString()), times(2));
    }
    // verify that connection was never created, and so preparedStatement is never called.
    verify(mockConnection, times(0)).prepareStatement(any());
  }

  @Test
  public void testConnectionRetryAndSuccess() throws StageException, SQLException {
    Connection mockConnection = mock(Connection.class);
    when(mockConnection.prepareStatement(any())).thenReturn(mock(PreparedStatement.class));
    Answer<Connection> exceptionAnswer = new Answer<>() {
      private int count = 0;
      @Override
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        if (count++ < 1) {
          throw new SQLException("Connection failed");
        }
        return mockConnection;
      }
    };

    try (MockedStatic<DriverManager> mockedDriverManager = mockStatic(DriverManager.class)) {
      mockedDriverManager.when(() -> DriverManager.getConnection(anyString(), anyString(), anyString()))
          .thenAnswer(exceptionAnswer);

      Stage stage = factory.get("QueryDatabaseTest/meal.conf");
      // verify that getConnection was called twice first attempt and then retry attempt
      mockedDriverManager.verify(() -> DriverManager.getConnection(anyString(), anyString(), anyString()), times(2));
    }
    // verify that connection has been established, and that prepareStatement was called
    verify(mockConnection, times(1)).prepareStatement(any());
  }
}
