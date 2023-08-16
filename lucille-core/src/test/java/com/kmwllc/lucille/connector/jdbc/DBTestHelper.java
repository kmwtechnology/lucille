package com.kmwllc.lucille.connector.jdbc;

import org.h2.tools.RunScript;
import org.junit.rules.ExternalResource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database helper class to create an h2 database and initialize it with a sql start script.
 *
 * <p>Created by matt on 3/27/17.
 */
public class DBTestHelper extends ExternalResource {
  private final String driver;
  private final String connection;
  private final String user;
  private final String password;
  private final String startScriptPath;
  private final String endScriptPath;
  private Connection conn;

  public DBTestHelper(
      String driver,
      String connection,
      String user,
      String password,
      String startScriptPath,
      String endScriptPath) {
    this.driver = driver;
    this.connection = connection;
    this.user = user;
    this.password = password;
    this.startScriptPath = startScriptPath;
    this.endScriptPath = endScriptPath;
  }

  @Override
  protected void before() throws Throwable {
    super.before();
    Class.forName("org.h2.Driver");
    conn = DriverManager.getConnection(connection);
    InputStream in = DBTestHelper.class.getClassLoader().getResourceAsStream(startScriptPath);
    RunScript.execute(conn, new InputStreamReader(in));
    conn.commit();
  }

  @Override
  protected void after() {
    super.after();
    try {
      InputStream in = DBTestHelper.class.getClassLoader().getResourceAsStream(endScriptPath);
      RunScript.execute(conn, new InputStreamReader(in));
      conn.commit();
      conn.close();
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }
}
