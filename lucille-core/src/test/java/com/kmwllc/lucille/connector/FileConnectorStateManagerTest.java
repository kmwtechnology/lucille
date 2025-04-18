package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.h2.tools.RunScript;
import org.junit.Rule;
import org.junit.Test;

public class FileConnectorStateManagerTest {

  private static final String helloFile = "/hello.txt";
  private static final String infoFile = "/files/info.txt";
  private static final String secretsFile = "/files/subdir/secrets.txt";

  private static final List<String> allFilePaths = List.of(helloFile, infoFile, secretsFile);

  private static final String helloQuery = "SELECT * FROM \"FILE\" WHERE name = '/hello.txt'";
  private static final String infoQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/info.txt'";
  private static final String secretsQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/subdir/secrets.txt'";

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "", "",
      "sm-db-test-start.sql", "sm-db-test-end.sql");

  @Test
  public void testStateManagerRootDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    // making sure we have times available...
    assertNotNull(manager.getLastPublished(helloFile));
    assertNotNull(manager.getLastPublished(infoFile));
    assertNotNull(manager.getLastPublished(secretsFile));

    // everything is encountered + all files are published.
    for (String filePath : allFilePaths) {
      manager.markFileEncountered(filePath);
      manager.successfullyPublishedFile(filePath);
    }

    // All of the files should have new (recent) timestamps.
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {

      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp should be within the last ~15 seconds.
      assertTrue(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));

      assertTrue(infoRS.next());
      Timestamp infoTimestamp = infoRS.getTimestamp("last_published");
      assertTrue(infoTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerOnNewFiles() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    for (String filePath : allFilePaths) {
      manager.markFileEncountered(filePath);
      manager.successfullyPublishedFile(filePath);
    }

    manager.markFileEncountered("/newdir/info.txt");
    manager.successfullyPublishedFile("/newdir/info.txt");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet infoRS = RunScript.execute(connection, new StringReader("SELECT * FROM file WHERE name = '/newdir/info.txt'"))) {

      assertTrue(infoRS.next());
      Timestamp newFileTimestamp = infoRS.getTimestamp("last_published");
      // the timestamp should be within the last ~15 seconds.
      assertTrue(newFileTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerWithDeletion() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    // pretending we got to the root of the file system and found nothing else..
    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());

    // reconnect, allowing us to make sure the deletions took place...
    manager.init();

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {
      // there shouldn't be any results for any of these... they were all deleted
      assertFalse(helloRS.next());
      assertFalse(infoRS.next());
      assertFalse(secretRS.next());
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  // Want to make sure that the StateManager can create appropriate tables if they didn't already exist.
  @Test
  public void testStateManagerOnNewTable() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    // has "tableName: "S3""
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/s3.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    // /files/info.txt --> s3://lucille-bucket/files/info.txt
    for (String filePath : allFilePaths) {
      manager.markFileEncountered("s3://lucille-bucket" + filePath);
      manager.successfullyPublishedFile("s3://lucille-bucket" + filePath);
    }

    // now, to run some queries and make sure that the data is all correct...
    String baseQuery = "SELECT * FROM \"S3\" WHERE name = ";
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/hello.txt'"));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/info.txt'"));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/subdir/secrets.txt'"))) {
      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp should be within the last ~15 seconds.
      assertTrue(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));

      assertTrue(infoRS.next());
      Timestamp infoTimestamp = infoRS.getTimestamp("last_published");
      assertTrue(infoTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testTraversalWithState() throws Exception {
    // for now, have no file options - just handle the files as is, no archives / file handlers, etc.
    String fileConnectorExampleDir = Paths.get("src/test/resources/FileConnectorTest/Example").toAbsolutePath() + "/";
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf");

    TestMessenger messenger1 = new TestMessenger();
    TestMessenger messenger2 = new TestMessenger();
    Publisher publisher1 = new PublisherImpl(config, messenger1, "run", "pipeline1");
    Publisher publisher2 = new PublisherImpl(config, messenger2, "run", "pipeline1");

    Connector connector = new FileConnector(config);
    connector.execute(publisher1);
    // See above - textExampleTraversal has 16 files. This has 18, since we don't exclude "skipFile.txt".
    assertEquals(18, messenger1.getDocsSentForProcessing().size());

    Timestamp publishedTimestamp;
    String baseQuery = "SELECT * FROM \"FILE-CONNECTOR-1\" WHERE name = '" + fileConnectorExampleDir;

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet aJSONResults = RunScript.execute(connection, new StringReader(baseQuery + "a.json'"));
        ResultSet subdirCSVResults = RunScript.execute(connection, new StringReader(baseQuery + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'"))) {

      assertTrue(aJSONResults.next());
      publishedTimestamp = aJSONResults.getTimestamp("last_published");
      assertTrue(publishedTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));

      assertTrue(subdirCSVResults.next());
      assertEquals(publishedTimestamp, subdirCSVResults.getTimestamp("last_published"));

      String baseUpdate = "UPDATE \"FILE-CONNECTOR-1\" SET last_published='2022-01-01 14:30:00' WHERE name='" + fileConnectorExampleDir;
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(baseUpdate + "a.json'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'");
      }
    }

    // and now... do it again... but should only have the two files that we manually modified above get processed.
    connector.execute(publisher2);
    System.out.println(messenger2.getDocsSentForProcessing());
    // 1 doc from json, 3 from the csv in the archive
    assertEquals(4, messenger2.getDocsSentForProcessing().size());

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet aJSONResults = RunScript.execute(connection, new StringReader(baseQuery + "a.json'"));
        ResultSet subdirCSVResults = RunScript.execute(connection, new StringReader(baseQuery + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'"))) {

      // a json's timestamp shouldn't have change
      assertTrue(aJSONResults.next());
      assertEquals(publishedTimestamp, aJSONResults.getTimestamp("last_published"));

      // the archive file entry's timestamp will have changed, but should still be recent
      assertTrue(subdirCSVResults.next());
      Timestamp subdirCSVTimestamp = subdirCSVResults.getTimestamp("last_published");
      assertTrue(subdirCSVTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
    }
  }
}
