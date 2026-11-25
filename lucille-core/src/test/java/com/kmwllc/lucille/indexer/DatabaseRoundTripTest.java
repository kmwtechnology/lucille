package com.kmwllc.lucille.indexer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Runner;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.h2.api.Trigger;
import org.junit.Rule;
import org.junit.Test;

/**
 * Copies a table with the DatabaseConnector and the DatabaseIndexer in a local run, then checks in the database that
 * the copy is identical to the original, so that values read by the connector are written back unchanged.
 */
public class DatabaseRoundTripTest {

  // also checks that the run closes the connector's and the indexer's connections
  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("DatabaseRoundTripTest/start.sql");

  /**
   * Records the id of each row inserted into target with the transaction that inserted it. It runs after the insert,
   * when the transaction has an id; a column default is evaluated too early for the first row of a transaction.
   */
  public static class RecordTransaction implements Trigger {

    @Override
    public void fire(Connection conn, Object[] oldRow, Object[] newRow) throws SQLException {
      try (PreparedStatement statement = conn.prepareStatement("INSERT INTO writes VALUES (?, TRANSACTION_ID())")) {
        statement.setObject(1, newRow[0]);
        statement.executeUpdate();
      }
    }
  }

  @Test
  public void testCopiedTableIsIdentical() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("DatabaseRoundTripTest/config.conf");

    // a local run, not test mode: test mode skips indexing, so nothing would be written to the target table
    assertTrue(Runner.run(config, Runner.RunType.LOCAL).getStatus());

    assertEquals(100, count("SELECT COUNT(*) FROM target"));
    // EXCEPT compares whole rows and treats two NULLs as equal, so both directions are empty only if the rows match
    assertEquals(0, count("SELECT COUNT(*) FROM (SELECT * FROM source EXCEPT SELECT * FROM target)"));
    assertEquals(0, count("SELECT COUNT(*) FROM (SELECT * FROM target EXCEPT SELECT * FROM source)"));

    // indexer.batchSize is 10, so no transaction wrote more than 10 rows, and the 100 rows took at least 10. Usually it
    // is exactly 10 of 10, but a pause longer than indexer.batchTimeoutMs sends a partly filled batch, so the test
    // doesn't depend on that.
    assertEquals(100, count("SELECT COUNT(*) FROM writes"));
    assertEquals(0, count("SELECT COUNT(*) FROM (SELECT tx FROM writes GROUP BY tx HAVING COUNT(*) > 10)"));
    assertTrue(count("SELECT COUNT(DISTINCT tx) FROM writes") >= 10);
  }

  private static int count(String query) throws Exception {
    try (Connection connection = DriverManager.getConnection(DBTestHelper.CONNECTION_STRING);
        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery(query)) {
      rs.next();
      return rs.getInt(1);
    }
  }
}
