package com.kmwllc.lucille.stage;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.doThrow;
import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import java.nio.charset.StandardCharsets;
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class QueryDatabaseTest {

  StageFactory factory = StageFactory.of(QueryDatabase.class);
  private static TimeZone originalTimeZone;

  @BeforeClass
  static public void beforeClass() {
    originalTimeZone = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
  }

  @AfterClass
  static public void afterClass() {
    TimeZone.setDefault(originalTimeZone);
  }

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
            "sql"),
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
    /*
      types to test: varchar_col, char_col, longvarchar_col, nchar_col, nvarchar_col, longnvarchar_col, clob_col, nclob_col,
      tinyint_col, smallint_col, integer_col, bigint_col, double_col, decimal_col, numeric_col, float_col, real_col,
      boolean_col, bit_col, date_col, timestamp_col, timestamp_w_timezone_col, blob_col, binary_col, varbinary_col, longvarbinary_col,
      nullable_int, nullable_varchar, nullable_date
    */

    // testing out d1
    assertEquals("Test VARCHAR", d1.getString("varchar_col"));
    // calling trim as char_col is storing fixed number of characters
    assertEquals("CHAR Test", d1.getString("char_col").trim());
    assertEquals("Long VARCHAR test data", d1.getString("longvarchar_col"));
    // calling trim as nchar_col is storing fixed number of characters
    assertEquals("\uD83D\uDE00", d1.getString("nchar_col").trim());
    assertEquals("こんにちは、世界！", d1.getString("nvarchar_col"));
    assertEquals("こんにちは、世界！長いテキストのテストです。", d1.getString("longnvarchar_col"));
    assertEquals("test clob", d1.getString("clob_col"));
    assertEquals("test nclob", d1.getString("nclob_col"));
    assertEquals(Integer.valueOf(127), d1.getInt("tinyint_col"));
    assertEquals(Integer.valueOf(32767), d1.getInt("smallint_col"));
    assertEquals(Integer.valueOf(2147483647), d1.getInt("integer_col"));
    assertEquals(Long.valueOf("9223372036854775807"), d1.getLong("bigint_col"));
    assertEquals(Double.valueOf(3.14159265359), d1.getDouble("double_col"));
    assertEquals(Double.valueOf(9877.0), d1.getDouble("decimal_col"));
    assertEquals(Double.valueOf(1.0), d1.getDouble("numeric_col"));
    assertEquals(Float.valueOf("2.71828"), d1.getFloat("float_col"));
    assertEquals(Float.valueOf("1.414214"), d1.getFloat("real_col"));
    assertEquals(true, d1.getBoolean("boolean_col"));
    assertEquals(true, d1.getBoolean("bit_col"));
    // note that we have set the TimeZone in this test class to be UTC
    // if it is not set, then the values would change according to the JVM Timezone at runtime
    assertEquals("2024-07-30T00:00:00Z", d1.getInstant("date_col").toString());
    assertEquals("1970-01-01T00:00:01Z", d1.getInstant("timestamp_col").toString());
    assertEquals("1969-12-31T19:00:01Z", d1.getInstant("timestamp_w_timezone_col").toString());
    assertFalse(d1.has("nullable_int"));
    assertNull(d1.getString("nullable_varchar"));
    // date would not get added to docs if null
    assertFalse(d1.has("nullable_date"));
    // converting blob to Expected String
    assertEquals("This is a test blob.", new String(d1.getBytes("blob_col"), StandardCharsets.UTF_8));
    // converting binaryColBytes from array of 100 to 4 and comparing
    byte[] binaryColBytes = d1.getBytes("binary_col");
    byte[] expectedBytes = new byte[] {(byte)0xBE, (byte)0xEF, (byte)0xBE, (byte)0xEF};
    assertArrayEquals(expectedBytes, Arrays.copyOf(binaryColBytes, 4));

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
    assertEquals("Another", d2.getString("varchar_col"));
    // calling trim as char_col is storing fixed number of characters
    assertEquals("Test", d2.getString("char_col").trim());
    assertEquals("More long text here", d2.getString("longvarchar_col"));
    // calling trim as nchar_col is storing fixed number of characters
    assertEquals("\uD83D\uDE00", d2.getString("nchar_col").trim());
    assertEquals("안녕하세요, 세계!", d2.getString("nvarchar_col"));
    assertEquals("안녕하세요, 세계! 긴 텍스트 테스트입니다.", d2.getString("longnvarchar_col"));
    assertEquals("test clob", d2.getString("clob_col"));
    assertEquals("test nclob", d2.getString("nclob_col"));
    assertEquals(Integer.valueOf(-128), d2.getInt("tinyint_col"));
    assertEquals(Integer.valueOf(-32768), d2.getInt("smallint_col"));
    assertEquals(Integer.valueOf(-2147483648), d2.getInt("integer_col"));
    assertEquals(Long.valueOf("-9223372036854775808"), d2.getLong("bigint_col"));
    assertEquals(Double.valueOf(1.41421356237), d2.getDouble("double_col"));
    assertEquals(Double.valueOf(500.0), d2.getDouble("decimal_col"));
    assertEquals(Double.valueOf(100000.0), d2.getDouble("numeric_col"));
    assertEquals(Float.valueOf("1.61803"), d2.getFloat("float_col"));
    assertEquals(Float.valueOf("3.141592"), d2.getFloat("real_col"));
    assertEquals(false, d2.getBoolean("boolean_col"));
    assertEquals(false, d2.getBoolean("bit_col"));
    // note that we have set the TimeZone in this test class to be UTC
    // if it is not set, then the values would change according to the JVM Timezone at runtime
    assertEquals("2023-01-01T00:00:00Z", d2.getInstant("date_col").toString());
    assertEquals("2038-01-19T03:14:07Z", d2.getInstant("timestamp_col").toString());
    assertEquals("2038-01-19T08:14:07Z", d2.getInstant("timestamp_w_timezone_col").toString());
    assertFalse(d2.has("nullable_int"));
    assertNull(d2.getString("nullable_varchar"));
    // date would not get added to docs if null
    assertFalse(d2.has("nullable_date"));
    // converting blob to Expected String
    assertEquals("This is a test blob2.", new String(d2.getBytes("blob_col"), StandardCharsets.UTF_8));
    // converting binaryColBytes from array of 100 to 4 and comparing
    binaryColBytes = d2.getBytes("binary_col");
    expectedBytes = new byte[] {(byte)0xBE, (byte)0xEF, (byte)0xBE, (byte)0xEF};
    assertArrayEquals(expectedBytes, Arrays.copyOf(binaryColBytes, 4));

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
      System.out.println(d.asMap());
    } catch (StageException e) {
      return e.getMessage();
    }
    return "no Error found";
  }
}
