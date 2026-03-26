package com.kmwllc.lucille.connector.jdbc;

import static org.junit.Assert.assertEquals;

import org.h2.tools.RunScript;
import org.junit.rules.ExternalResource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Helper to create an H2 database and initialize it with a sql start script.
 *
 * Meant to be used as a @Rule in the context of a Junit test class as follows:
 *
 * <pre>
 *   @Rule
 *   public final DBTestHelper dbHelper = new DBTestHelper("db-test-start.sql");
 * </pre>
 *
 * With this declaration, a new instance of DBTestHelper will be created and initialized (by calling the before() method)
 * before each test method, and its after() method will be called after each test method.
 *
 * Note that H2 drops an in-memory database after the last connection closes, unless "DB_CLOSE_DELAY=-1"
 * is included in the connection string. Here, we do NOT include "DB_CLOSE_DELAY=-1" in the connection string but users
 * of DBTestHelper as a Junit @Rule can assume that the database will remain available throughout the duration of each
 * test because one connection will be held open by DBTestHelper itself. At the end of each test,
 * the last connection will be closed in after() and the database will be dropped. This assures that the database
 * is fully re-initialized between tests and no database state can persist across test boundaries.
 *
 * Note that a "start script" is supported but an "end script" is not supported because the database is dropped after each
 * test. Running an end script right before the database is dropped would have no point.
 */
public class DBTestHelper extends ExternalResource {

  public static final String CONNECTION_STRING = "jdbc:h2:mem:test";

  private final String startScriptPath;
  private Connection connection;

  public DBTestHelper(String startScriptPath) {
    this.startScriptPath = startScriptPath;
  }

  @Override
  protected void before() throws Throwable {
    super.before();

    connection = DriverManager.getConnection(CONNECTION_STRING);

    try (InputStream inputStream = getClass().getClassLoader().getResourceAsStream(startScriptPath)) {
      if (inputStream == null) {
        throw new IllegalArgumentException("Start script not found on classpath: " + startScriptPath);
      }
      try (InputStreamReader reader = new InputStreamReader(inputStream)) {
        RunScript.execute(connection, reader);
        connection.commit();
      }
    }

    // at the beginning of a test, the only open DB connection should be the one held by DBTestHelper
    assertEquals(1, countConnections());
  }

  @Override
  protected void after() {
    super.after();

    try {
      // at the end of a test, the only open DB connection should be the one held by DBTestHelper
      assertEquals(1, countConnections());
    } finally {
      try {
        connection.close();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public int countConnections() {
    try (ResultSet rs = RunScript.execute(connection, new StringReader("select count(*) from information_schema.sessions;"))) {
      rs.next();
      return rs.getInt(1);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
}
