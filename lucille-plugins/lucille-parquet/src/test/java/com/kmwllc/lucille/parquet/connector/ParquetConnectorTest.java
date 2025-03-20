package com.kmwllc.lucille.parquet.connector;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.Test;

public class ParquetConnectorTest {
  /*
  The data is:
  'name': ['Oliver', 'Mia', 'Jasper', 'Mr. Meow', 'Elijah', 'Spot'],
  'age': [20, 35, 46, 8, 7, 3],
  'net_worth': [10.0, 12.5, 7.5, 0.0, 15.3, -2.0],
  'species': ['Human', 'Human', 'Human', 'Cat', 'Human', 'Dog'],
  'id': ['1', '2', '3', '4', '5', '6'],
  'hobbies': [['Reading', 'Running'], ['Cooking', 'Painting'], ['Gaming', 'Reading'],
                 ['Sleeping', 'Chasing mice'], ['Drawing', 'Coding'], ['Playing', 'Sleeping']]

  The "with rows" file has three different row groups to read from, in order, each with length 2.
   */

  @Test
  public void testConnector() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/connector.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(6, docs.size());

    for (int i = 1; i <= docs.size(); i++) {
      Document nthDoc = docs.get(i - 1);
      assertEquals("" + i, nthDoc.getId());
    }
  }

  @Test
  public void testLimit() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/limit.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    for (int i = 1; i <= docs.size(); i++) {
      Document nthDoc = docs.get(i - 1);
      assertEquals("" + i, nthDoc.getId());
    }
  }

  @Test
  public void testStart() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    // Starts at index 3 in the parquet file
    Config config = ConfigFactory.load("ParquetConnectorTest/conf/start.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    for (int i = 0; i < docs.size(); i++) {
      Document nthDoc = docs.get(i);
      // Should get docs w/ ids 3, 4, 5, 6
      assertEquals("" + (i + 4), nthDoc.getId());
    }
  }

  @Test
  public void testStartAndLimit() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/startAndLimit.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(2, docs.size());

    for (int i = 0; i < docs.size(); i++) {
      Document nthDoc = docs.get(i);
      // Should get docs w/ ids 3, 4
      assertEquals("" + (i + 3), nthDoc.getId());
    }
  }

  // The following tests use the "with_row_groups" file...
  @Test
  public void testConnectorWithRows() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/connectorWithRows.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(6, docs.size());

    for (int i = 1; i <= docs.size(); i++) {
      Document nthDoc = docs.get(i - 1);
      assertEquals("" + i, nthDoc.getId());
    }
  }

  @Test
  public void testLimitWithRows() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/limitWithRows.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    for (int i = 1; i <= docs.size(); i++) {
      Document nthDoc = docs.get(i - 1);
      assertEquals("" + i, nthDoc.getId());
    }
  }

  @Test
  public void testStartWithRows() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/startWithRows.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    for (int i = 0; i < docs.size(); i++) {
      Document nthDoc = docs.get(i);
      // Should get docs w/ ids 4, 5, 6
      assertEquals("" + (i + 4), nthDoc.getId());
    }
  }

  @Test
  public void testStartAndLimitWithRows() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/startAndLimitWithRows.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(2, docs.size());

    for (int i = 0; i < docs.size(); i++) {
      Document nthDoc = docs.get(i);
      // Should get docs w/ ids 3, 4
      assertEquals("" + (i + 3), nthDoc.getId());
    }
  }

  // Some smaller, more specific cases
  @Test
  public void testSkipEntireFile() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/largeStart.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(0, docs.size());
  }

  @Test
  public void testSkipNonParquetFile() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.load("ParquetConnectorTest/conf/notParquet.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(0, docs.size());
  }
}
