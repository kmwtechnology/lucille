package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.net.URI;

import java.io.File;
import java.io.StringReader;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.FileUtils;
import org.h2.tools.RunScript;
import org.junit.Rule;
import org.junit.Test;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;

public class FileConnectorStateManagerTest {

  private static final String helloFile = "/hello.txt";
  private static final String infoFile = "/files/info.txt";
  private static final String secretsFile = "/files/subdir/secrets.txt";

  private static final List<String> allFilePaths = List.of(helloFile, infoFile, secretsFile);

  private static final String helloQuery = "SELECT * FROM \"FILE\" WHERE name = '/hello.txt'";
  private static final String infoQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/info.txt'";
  private static final String secretsQuery = "SELECT * FROM \"FILE\" WHERE name = '/files/subdir/secrets.txt'";

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("sm-db-test-start.sql");

  // Note that there is no manipulation of the LockRegistry even though each FCSM
  // is using the same (static) one - because calls to init / shutdown should be made appropriately...

  @Test
  public void testStateManagerRootDirectory() throws Exception {
    Instant start = Instant.now();

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
      // the timestamp should be after the start of the test
      assertTrue(helloTimestamp.after(Timestamp.from(start)));

      assertTrue(infoRS.next());
      Timestamp infoTimestamp = infoRS.getTimestamp("last_published");
      assertTrue(infoTimestamp.after(Timestamp.from(start)));

      assertTrue(secretRS.next());
      Timestamp secretTimestamp = secretRS.getTimestamp("last_published");
      assertTrue(secretTimestamp.after(Timestamp.from(start)));
    }

    manager.shutdown();
  }

  @Test
  public void testGetLastPublished() throws Exception {
    Instant start = Instant.now();

    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();
    for (String filePath : allFilePaths) {
      manager.markFileEncountered(filePath);
      manager.successfullyPublishedFile(filePath);
    }

    Instant helloLastPublished = manager.getLastPublished(helloFile);
    assertTrue(helloLastPublished.isAfter(start) && helloLastPublished.isBefore(Instant.now()));

    Instant infoLastPublished = manager.getLastPublished(infoFile);
    assertTrue(infoLastPublished.isAfter(start) && infoLastPublished.isBefore(Instant.now()));

    Instant secretsLastPublished = manager.getLastPublished(secretsFile);
    assertTrue(secretsLastPublished.isAfter(start) && secretsLastPublished.isBefore(Instant.now()));

    manager.shutdown();
  }

  @Test
  public void testStateManagerOnNewFiles() throws Exception {
    Instant start = Instant.now();

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
      Instant newInstant = infoRS.getObject("last_published", Instant.class);
      assertTrue(newInstant.isBefore(Instant.now()) && newInstant.isAfter(start));
    }

    manager.shutdown();
  }

  @Test
  public void testStateManagerWithDeletion() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);

    // pretending we got to the root of the file system and found everything, mark it all as encountered
    manager.init();

    manager.markFileEncountered("/hello.txt");
    manager.markFileEncountered("/files/info.txt");
    manager.markFileEncountered("/files/subdir/secrets.txt");

    manager.shutdown();
    assertEquals(1, dbHelper.countConnections());

    // reconnecting to run these queries + for second traversal
    manager.init();

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {
      // everything should still be here
      assertTrue(helloRS.next());
      assertTrue(infoRS.next());
      assertTrue(secretRS.next());
    }

    // pretending that "files" directory has been deleted - so we only see "hello.txt"
    manager.markFileEncountered("/hello.txt");

    // traversal complete
    manager.shutdown();
    assertEquals(1, dbHelper.countConnections());

    // reconnecting to run queries and ensure deletions took place
    manager.init();

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet helloRS = RunScript.execute(connection, new StringReader(helloQuery));
        ResultSet infoRS = RunScript.execute(connection, new StringReader(infoQuery));
        ResultSet secretRS = RunScript.execute(connection, new StringReader(secretsQuery))) {
      // there shouldn't be any results for the files under "files" directory
      assertTrue(helloRS.next());
      assertFalse(infoRS.next());
      assertFalse(secretRS.next());
    }

    manager.shutdown();
  }

  // Want to make sure that the StateManager can create appropriate tables if they didn't already exist.
  @Test
  public void testStateManagerOnNewTable() throws Exception {
    Instant start = Instant.now();

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
      Instant helloInstant = helloRS.getObject("last_published", Instant.class);
      assertTrue(helloInstant.isBefore(Instant.now()) && helloInstant.isAfter(start));

      assertTrue(infoRS.next());
      Instant infoInstant = infoRS.getObject("last_published", Instant.class);
      assertTrue(infoInstant.isBefore(Instant.now()) && infoInstant.isAfter(start));

      assertTrue(secretRS.next());
      Instant secretInstant = secretRS.getObject("last_published", Instant.class);
      assertTrue(secretInstant.isBefore(Instant.now()) && secretInstant.isAfter(start));     }

    manager.shutdown();
  }

  @Test
  public void testStateEnabledTrue() throws Exception {
    // we would like to make sure state gets enabled when we have state.enabled: true in the config
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("state.enabled", ConfigValueFactory.fromAnyRef(true));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector conn = new FileConnector(config);
    conn.execute(publisher);
    conn.close();

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        Statement stmt = connection.createStatement()) {
      ResultSet rs = stmt.executeQuery("SELECT * FROM \"FILE-CONNECTOR-1\"");
      assertNotNull(rs);
      System.out.println(rs);
    }
  }

  @Test(expected = SQLException.class)
  public void testStateDisabled() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("state.enabled", ConfigValueFactory.fromAnyRef(false));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector conn = new FileConnector(config);
    conn.execute(publisher);
    conn.close();

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        Statement stmt = connection.createStatement()) {
        stmt.executeQuery("SELECT * FROM \"FILE-CONNECTOR-1\"");
    }
  }

  // Full mode republishes everything on every run regardless of last_published; state DB is still updated.
  @Test
  public void testTraversalWithState() throws Exception {
    Instant start = Instant.now();

    String fileConnectorExampleDir = Paths.get("src/test/resources/FileConnectorTest/example").toUri().toString();
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf");

    TestMessenger messenger1 = new TestMessenger();
    TestMessenger messenger2 = new TestMessenger();
    Publisher publisher1 = new PublisherImpl(config, messenger1, "run", "pipeline1");
    Publisher publisher2 = new PublisherImpl(config, messenger2, "run", "pipeline1");

    Connector conn = new FileConnector(config);
    conn.execute(publisher1);
    conn.close();
    // See above - textExampleTraversal has 16 files. This has 18, since we don't exclude "skipFile.txt".
    assertEquals(18, messenger1.getDocsSentForProcessing().size());

    Timestamp publishedTimestamp;
    String baseQuery = "SELECT * FROM \"FILE-CONNECTOR-1\" WHERE name = '" + fileConnectorExampleDir;

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet aJSONResults = RunScript.execute(connection, new StringReader(baseQuery + "a.json'"));
        ResultSet subdirCSVResults = RunScript.execute(connection, new StringReader(baseQuery + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'"))) {

      assertTrue(aJSONResults.next());
      publishedTimestamp = aJSONResults.getTimestamp("last_published");
      assertTrue(publishedTimestamp.after(Timestamp.from(start)));

      assertTrue(subdirCSVResults.next());
      assertEquals(publishedTimestamp, subdirCSVResults.getTimestamp("last_published"));

      // before we run again, we will set these two files to have been published a long time ago, while everything else will still be
      // very recently published.
      String baseUpdate = "UPDATE \"FILE-CONNECTOR-1\" SET last_published='2022-01-01 14:30:00' WHERE name='" + fileConnectorExampleDir;
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(baseUpdate + "a.json'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'");
      }
    }

    // second run in default full mode republishes all docs.
    conn.execute(publisher2);
    conn.close();
    assertEquals(18, messenger2.getDocsSentForProcessing().size());
  }

  // empty state configuration should mean we create a database for the user.
  @Test
  public void testEmbeddedCreationEmptyConfig() throws Exception {
    File stateDirectory = new File("state");
    File dbFile = new File("state/connector.mv.db");

    assertFalse(stateDirectory.isDirectory());
    assertFalse(dbFile.isFile());
    Config emptyConfig = ConfigFactory.empty();

    FileConnectorStateManager stateMgr = new FileConnectorStateManager(emptyConfig, "connector");

    try {
      stateMgr.init();

      assertTrue(stateDirectory.isDirectory());
      assertTrue(dbFile.isFile());
    } finally {
      stateMgr.shutdown();

      FileUtils.deleteDirectory(stateDirectory);
    }
  }

  @Test
  public void testMarkAllEntriesEncountered() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    // insert archive entries and an unrelated archive entry directly, all with encountered=false
    String archivePrefix = "file:///tmp/archive.zip!";
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        Statement stmt = connection.createStatement()) {
      stmt.executeUpdate("INSERT INTO \"FILE\" VALUES ('" + archivePrefix + "entry1.txt', NULL, FALSE)");
      stmt.executeUpdate("INSERT INTO \"FILE\" VALUES ('" + archivePrefix + "subdir/entry2.txt', NULL, FALSE)");
      stmt.executeUpdate("INSERT INTO \"FILE\" VALUES ('file:///tmp/other.zip!entry.txt', NULL, FALSE)");
    }

    manager.markAllEntriesEncountered(archivePrefix);

    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        Statement stmt = connection.createStatement()) {
      // every matching archive entry should be encountered
      ResultSet rs1 = stmt.executeQuery("SELECT encountered FROM \"FILE\" WHERE name = '" + archivePrefix + "entry1.txt'");
      assertTrue(rs1.next());
      assertTrue(rs1.getBoolean("encountered"));

      ResultSet rs2 = stmt.executeQuery("SELECT encountered FROM \"FILE\" WHERE name = '" + archivePrefix + "subdir/entry2.txt'");
      assertTrue(rs2.next());
      assertTrue(rs2.getBoolean("encountered"));

      // entry from a diff archive shouldn't be encountered
      ResultSet rs3 = stmt.executeQuery("SELECT encountered FROM \"FILE\" WHERE name = 'file:///tmp/other.zip!entry.txt'");
      assertTrue(rs3.next());
      assertFalse(rs3.getBoolean("encountered"));

      // preexisting non-archive entries should also not have been encountered
      ResultSet rs4 = stmt.executeQuery("SELECT encountered FROM \"FILE\" WHERE name = '/hello.txt'");
      assertTrue(rs4.next());
      assertFalse(rs4.getBoolean("encountered"));
    }

    manager.shutdown();
  }

  // FileConnector tests taking advantage of our embedded test database
  @Test
  public void testTraversalWithStateAndExclusion() throws Exception {
    String fileConnectorExampleDir = Paths.get("src/test/resources/FileConnectorTest/example").toUri().toString();
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/stateAndExclude.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run", "pipeline1");

    Connector conn = new FileConnector(config);
    conn.execute(publisher);
    conn.close();
    assertEquals(16, messenger.getDocsSentForProcessing().size());

    // Even though "skipFile.txt" is excluded, we should still have a state entry for it, without a lastPublished time
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet skipFile1Results = RunScript.execute(connection, new StringReader("SELECT * FROM \"FILE-CONNECTOR-1\" WHERE name='" + fileConnectorExampleDir + "skipFile.txt'"));
        ResultSet skipFile2Results = RunScript.execute(connection, new StringReader("SELECT * FROM \"FILE-CONNECTOR-1\" WHERE name='" + fileConnectorExampleDir + "subdir/skipFile.txt'"))) {

      assertTrue(skipFile1Results.next());
      assertNull(skipFile1Results.getTimestamp("last_published"));

      assertTrue(skipFile2Results.next());
      assertNull(skipFile2Results.getTimestamp("last_published"));
    }
  }

  @Test
  public void testTraversalMultipleFileTypesWithIncrementalMode() throws Exception {
    // Verifies incremental mode with tombstones enabled across a directory that contains both plain
    // files and archives. state.conf enables handleArchivedFiles and handleCompressedFiles, so the
    // connector tracks archive entries individually in the state DB alongside their container files.
    //
    // The directory contains:
    //   Plain files:  a.json, skipFile.txt, subdir/c.jsonl, subdir/e.yaml, subdir/skipFile.txt
    //   Compressed:   helloWorld.txt.gz  -> DB entry: helloWorld.txt.gz!helloWorld.txt
    //                 subdir/b.json.gz   -> DB entry: subdir/b.json.gz!b.json
    //   Archives:     subDirWith2TxtFiles.zip     -> entries: first.txt, second.txt
    //                                               (directory entry and macOS hidden file excluded)
    //                 subdirWith1csv1xml.tar.gz   -> entries: default.csv (CSV handler), staff.xml (XML handler)
    //                                               (directory entry skipped)
    //
    // First run publishes 18 docs total: plain files + archive entries + file-handler output rows.
    // All files and their archive entries are recorded in the state DB with a fresh last_published.
    String fileConnectorExampleDir = Paths.get("src/test/resources/FileConnectorTest/example").toUri().toString();
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorTest/state.conf")
        .withValue("filterOptions.publishMode", ConfigValueFactory.fromAnyRef("incremental"))
        .withValue("filterOptions.sendTombstones", ConfigValueFactory.fromAnyRef("true"));

    TestMessenger messenger1 = new TestMessenger();
    TestMessenger messenger2 = new TestMessenger();
    Publisher publisher1 = new PublisherImpl(config, messenger1, "run", "pipeline1");
    Publisher publisher2 = new PublisherImpl(config, messenger2, "run", "pipeline1");

    Connector conn = new FileConnector(config);
    conn.execute(publisher1);
    conn.close();
    assertEquals(18, messenger1.getDocsSentForProcessing().size());

    // Simulate two files being stale by changing their last_published to 2022, making them appear older than the files on disk so
    // they are re-published on the second run
    // - a.json: plain file
    // - subdirWith1csv1xml.tar.gz (which has default.csv with 3 docs)
    // All other files/entries were just published, so their last_published is newer than the files on disk and
    // modifiedSinceLastPublish correctly skips them
    String baseQuery = "SELECT * FROM \"FILE-CONNECTOR-1\" WHERE name = '" + fileConnectorExampleDir;
    try (Connection connection = DriverManager.getConnection("jdbc:h2:mem:test", "", "");
        ResultSet aJSONResults = RunScript.execute(connection, new StringReader(baseQuery + "a.json'"));
        ResultSet subdirCSVResults = RunScript.execute(connection, new StringReader(baseQuery + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'"))) {
      assertTrue(aJSONResults.next());
      assertTrue(subdirCSVResults.next());
      String baseUpdate = "UPDATE \"FILE-CONNECTOR-1\" SET last_published='2022-01-01 14:30:00' WHERE name='" + fileConnectorExampleDir;
      try (Statement statement = connection.createStatement()) {
        statement.executeUpdate(baseUpdate + "a.json'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz'");
        statement.executeUpdate(baseUpdate + "subdirWith1csv1xml.tar.gz!subdirWith1csv1xml/default.csv'");
      }
    }

    // Second run: only the aged files are re-published (4 docs total), no tombstones.
    // The three unchanged archives (helloWorld.txt.gz, subDirWith2TxtFiles.zip, subdir/b.json.gz)
    // are visited but skipped by includeFile. Their entries are marked encountered via
    // markAllEntriesEncountered, so they are not seen as expired and no tombstones are generated.
    conn.execute(publisher2);
    conn.close();
    List<Document> secondRunDocs = messenger2.getDocsSentForProcessing();
    long tombstoneCount = secondRunDocs.stream()
        .filter(doc -> Boolean.TRUE.equals(doc.getBoolean(FileConnector.EXPIRED)))
        .count();
    assertEquals(4, secondRunDocs.size());
    assertEquals(0, tombstoneCount);
  }

  @Test
  public void testListExpiredFiles() throws Exception {
    // Directly tests listExpiredFiles()
    Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    // Mark hello and info as encountered; leave secrets unvisited (encountered = false).
    manager.markFileEncountered(helloFile);
    manager.markFileEncountered(infoFile);

    List<URI> expired = manager.listExpiredFiles();
    assertEquals(1, expired.size());
    assertTrue(expired.contains(URI.create(secretsFile)));

    manager.shutdown();
  }

  @Test
  public void testPreventConcurrentConnections() throws Exception {
    final Config config = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/config.conf");
    final Config altConfig = ConfigFactory.parseResourcesAnySyntax("FileConnectorStateManagerTest/alt_conn_str.conf");

    FileConnectorStateManager manager = new FileConnectorStateManager(config, null);
    manager.init();

    CountDownLatch initCompleted = new CountDownLatch(1);

    Thread t = new Thread(() -> {
      try {
        FileConnectorStateManager manager2 = new FileConnectorStateManager(config, null);

        // blocking as long as first manager is up
        manager2.init();
        initCompleted.countDown();
        manager2.shutdown();
      } catch (Exception e) {
        fail("Error in thread: " + e.getMessage());
      }
    });

    // these calls should be non-blocking because
    // they are using an alternate connection string!
    FileConnectorStateManager manager3 = new FileConnectorStateManager(altConfig, null);
    manager3.init();
    manager3.shutdown();

    t.start();

    assertEquals(1, initCompleted.getCount());

    manager.shutdown();
    assertTrue(initCompleted.await(1, TimeUnit.SECONDS));
    t.join();
  }

}
