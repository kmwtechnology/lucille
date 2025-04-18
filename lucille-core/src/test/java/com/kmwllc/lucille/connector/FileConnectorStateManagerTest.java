package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.when;

import com.kmwllc.lucille.connector.FileConnectorStateManager.FileConnectorState;
import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.StringReader;
import java.net.URI;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.h2.tools.RunScript;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class FileConnectorStateManagerTest {

  private static final String helloFile = "/hello.txt";
  private static final String infoFile = "/files/info.txt";
  private static final String secretsFile = "/files/subdir/secrets.txt";

  private static final String filesDirectory = "/files/";
  private static final String subdirDirectory = "/files/subdir/";

  private static final List<String> allFilePaths = List.of(helloFile, infoFile, secretsFile);
  private static final List<String> allFileAndDirectoryPaths = List.of(helloFile, infoFile, secretsFile, "/", filesDirectory, subdirDirectory);

  private static final String helloQuery = "SELECT * FROM \"FILE\" WHERE name = '/hello.txt'";
  private static final String infoQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/info.txt'";
  private static final String secretsQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/subdir/secrets.txt'";

  private static final String filesQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/'";
  private static final String subdirQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/subdir/'";

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "", "",
      "sm-db-test-start.sql", "sm-db-test-end.sql");

  @Test
  public void testStateManagerRootDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    FileConnectorState state = manager.getStateForTraversal("file");

    // making sure the times are read correctly from the start file...
    assertNotNull(state.getLastPublished(helloFile));
    assertNotNull(state.getLastPublished(infoFile));
    assertNotNull(state.getLastPublished(secretsFile));

    // everything is encountered + all files are published.
    for (String filePath : allFileAndDirectoryPaths) {
      state.markFileOrDirectoryEncountered(filePath);
    }

    for (String filePath : allFilePaths) {
      state.successfullyPublishedFile(filePath);
    }

    manager.updateDatabaseAfterTraversal(URI.create("/"), "file");

    // All of the files should have new (recent) timestamps.
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {

      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp should be within the last ~15 seconds.
      assertTrue(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
      assertEquals("/", helloRS.getString("parent"));

      assertTrue(infoRS.next());
      Timestamp infoTimestamp = infoRS.getTimestamp("last_published");
      assertTrue(infoTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
      assertEquals(filesDirectory, infoRS.getString("parent"));

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(15L))));
      assertEquals(subdirDirectory, secretRS.getString("parent"));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerSubdirDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    FileConnectorState state = manager.getStateForTraversal("file");

    state.markFileOrDirectoryEncountered(subdirDirectory);
    state.markFileOrDirectoryEncountered(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    manager.updateDatabaseAfterTraversal(URI.create(subdirDirectory), "file");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      // the timestamp should be within the last 6 seconds.
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(6))));

      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp SHOULDN'T be within the last ~60 seconds - it wasn't published!
      assertFalse(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(60))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerSingleFile() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    FileConnectorState state = manager.getStateForTraversal( "file");

    state.markFileOrDirectoryEncountered(infoFile);
    state.successfullyPublishedFile(infoFile);

    manager.updateDatabaseAfterTraversal(URI.create(infoFile), "file");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery))) {

      assertTrue(infoRS.next());
      Timestamp secretTimestamp = infoRS.getTimestamp("last_published");
      // the timestamp should be within the last 6 seconds.
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(6))));

      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp SHOULDN'T be within the last ~60 seconds, it wasn't published!
      assertFalse(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(60))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerOnNewDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    FileConnectorState state = manager.getStateForTraversal("file");

    state.markFileOrDirectoryEncountered("/newdir/");
    state.markFileOrDirectoryEncountered("/newdir/info.txt");
    state.successfullyPublishedFile("/newdir/info.txt");

    manager.updateDatabaseAfterTraversal(URI.create("/newdir/"), "file");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet newdirRS = RunScript.execute(connection, new StringReader("SELECT * FROM file WHERE name = '/newdir/'"));
        ResultSet infoRS = RunScript.execute(connection, new StringReader("SELECT * FROM file WHERE name = '/newdir/info.txt'"))) {

      assertTrue(newdirRS.next());
      assertNull(newdirRS.getTimestamp("last_published"));
      assertTrue(newdirRS.getBoolean("is_directory"));

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
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    // a "full" traversal / from the root directory
    FileConnectorState state = manager.getStateForTraversal("file");

    // should have state for each files
    assertNotNull(state.getLastPublished(helloFile));
    assertNotNull(state.getLastPublished(infoFile));
    assertNotNull(state.getLastPublished(secretsFile));

    // pretendng we got to the root of the file system and found nothing.
    state.markFileOrDirectoryEncountered("/");

    manager.updateDatabaseAfterTraversal(URI.create("/"), "file");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery));
        ResultSet filesDirRS = RunScript.execute(connection, new StringReader(filesQuery));
        ResultSet subdirRS = RunScript.execute(connection, new StringReader(subdirQuery))) {
      // there shouldn't be any results for any of these... they were all deleted
      assertFalse(helloRS.next());
      assertFalse(infoRS.next());
      assertFalse(secretRS.next());
      assertFalse(filesDirRS.next());
      assertFalse(subdirRS.next());
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testStateManagerSubdirWithoutFinalSlash() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    // not using a final slash in the URI here...
    FileConnectorState state = manager.getStateForTraversal("file");

    state.markFileOrDirectoryEncountered(subdirDirectory);
    state.markFileOrDirectoryEncountered(secretsFile);
    state.successfullyPublishedFile(secretsFile);

    manager.updateDatabaseAfterTraversal(URI.create(subdirDirectory), "file");

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      // the timestamp should be within the last 6 seconds.
      assertTrue(secretTimestamp.after(Timestamp.from(Instant.now().minusSeconds(6))));

      assertTrue(helloRS.next());
      Timestamp helloTimestamp = helloRS.getTimestamp("last_published");
      // the timestamp SHOULDN'T be within the last ~60 seconds - it wasn't published!
      assertFalse(helloTimestamp.after(Timestamp.from(Instant.now().minusSeconds(60))));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  // Want to make sure that the StateManager can create appropriate tables if they didn't already exist.
  @Test
  public void testStateManagerOnNewTable() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config);
    manager.init();

    // this table won't exist, but this method call should create it
    FileConnectorState state = manager.getStateForTraversal("s3_lucille-bucket");

    // /files/ --> s3://lucille-bucket/files/
    for (String filePath : allFileAndDirectoryPaths) {
      state.markFileOrDirectoryEncountered("s3://lucille-bucket" + filePath);
    }

    // /files/info.txt --> s3://lucille-bucket/files/info.txt
    for (String filePath : allFilePaths) {
      state.successfullyPublishedFile("s3://lucille-bucket" + filePath);
    }

    manager.updateDatabaseAfterTraversal(URI.create("s3://lucille-bucket/"), "s3_lucille-bucket");

    // now, to run some queries and make sure that the data is all correct...
    String baseQuery = "SELECT * FROM \"S3_LUCILLE-BUCKET\" WHERE name = ";
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet bucketRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/'"));
        ResultSet filesDirRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/'"));
        ResultSet subdirRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/subdir/'"));
        ResultSet helloRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/hello.txt'"));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/info.txt'"));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(baseQuery + "'s3://lucille-bucket/files/subdir/secrets.txt'"))) {

      assertTrue(bucketRS.next());
      assertTrue(filesDirRS.next());
      assertTrue(subdirRS.next());
      assertTrue(helloRS.next());
      assertTrue(infoRS.next());
      assertTrue(secretRS.next());

      // Also want to make sure that "parent" is still handled appropriately. We don't check bucketRS's parent, because it
      // doesn't really matter (you never traverse from the root of ALL of S3, and want to see all the buckets indexed to the root... ;) )
      assertEquals("s3://lucille-bucket/", filesDirRS.getString("parent"));
      assertEquals("s3://lucille-bucket/files/", subdirRS.getString("parent"));
      assertEquals("s3://lucille-bucket/", helloRS.getString("parent"));
      assertEquals("s3://lucille-bucket/files/", infoRS.getString("parent"));
      assertEquals("s3://lucille-bucket/files/subdir/", secretRS.getString("parent"));
    }

    manager.shutdown();
    assertEquals(1, dbHelper.checkNumConnections());
  }

  @Test
  public void testTraversalWithState() throws Exception {
    // for now, have no file options - just handle the files as is, no archives / file handlers, etc.
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf");
    config = config.withFallback(ConfigFactory.parseMap(Map.of("pathToStorage", Paths.get("./src/test/resources/FileConnectorTest/Example").toAbsolutePath().toString())));

    TestMessenger messenger1 = new TestMessenger();
    TestMessenger messenger2 = new TestMessenger();
    Publisher publisher1 = new PublisherImpl(config, messenger1, "run", "pipeline1");
    Publisher publisher2 = new PublisherImpl(config, messenger2, "run", "pipeline1");

    Connector connector = new FileConnector(config);
    connector.execute(publisher1);
    // See above - textExampleTraversal has 16 files. This has 18, since we don't exclude "skipFile.txt".
    assertEquals(18, messenger1.getDocsSentForProcessing().size());

    // TODO: add checks on the contents of the database for the files.

    // and now... do it again... but shouldn't have anything published / sent for processing, since our cutoff is 1h.
    connector.execute(publisher2);
    assertEquals(0, messenger2.getDocsSentForProcessing().size());

    // TODO: add checks on the contents of the database for the files.
  }
}
