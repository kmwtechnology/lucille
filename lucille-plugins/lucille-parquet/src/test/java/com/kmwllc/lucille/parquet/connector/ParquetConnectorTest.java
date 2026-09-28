package com.kmwllc.lucille.parquet.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.Test;

public class ParquetConnectorTest {
  /*
  In example.parquet, the data is:

  'name': ['Oliver', 'Mia', 'Jasper', 'Mr. Meow', 'Elijah', 'Spot'],
  'age': [20, 35, 46, 8, 7, 3],
  'net_worth': [10.0, 12.5, 7.5, 0.0, 15.3, -2.0],
  'species': ['Human', 'Human', 'Human', 'Cat', 'Human', 'Dog'],
  'id': ['1', '2', '3', '4', '5', '6'],
  'hobbies': [['Reading', 'Running'], ['Cooking', 'Painting'], ['Gaming', 'Reading'],
                 ['Sleeping', 'Chasing mice'], ['Drawing', 'Coding'], ['Playing', 'Sleeping']]

  Hobbies, a list of lists, is an example of a non-primitive field.

  The "with rows" file has three different row groups to read from, in order, each with length 2. Otherwise, it is the same data.

  In long_example.parquet, there is data with the same schema, but there are 25 entries, with IDs 1, 2, ..., 24, 25.
   */

  // Starting with some more fine-grained tests on just a single file w/ the connector
  @Test
  public void testConnector() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/connector.conf");
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

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/limit.conf");
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
    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/start.conf");
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

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/startAndLimit.conf");
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

  @Test
  public void testTraversal() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/traversal.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(37, docs.size());
  }

  @Test
  public void testTraversalWithStart() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/traversalWithStart.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(31, docs.size());
  }

  @Test
  public void testTraversalWithLimit() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/traversalWithLimit.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(15, docs.size());
  }

  // Start = 2 - so we hit the limit, still have 31 documents in total
  @Test
  public void testTraversalWithStart2AndLimit() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/traversalWithStart2AndLimit.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(15, docs.size());
  }

  // Start = 20, so we only have 5 documents to get from the "long_example" file
  @Test
  public void testTraversalWithStart30AndLimit() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/traversalWithStart20AndLimit.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(5, docs.size());
  }

  // Some smaller, more specific cases, that are somewhat covered by the "traversal" tests.
  // Making sure we will skip entire files as appropriate
  @Test
  public void testSkipEntireFile() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/largeStart.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(0, docs.size());
  }

  // Also making sure we will just skip non-parquet files as well.
  @Test
  public void testSkipNonParquetFile() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/notParquet.conf");
    ParquetConnector connector = new ParquetConnector(config);

    connector.execute(publisher);
    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(0, docs.size());
  }

  // Getting an IOException during traversal due to pointing it to a path / file that doesn't exist
  @Test
  public void testNonExistentFile() throws Exception {
    TestMessenger messenger = new TestMessenger();
    PublisherImpl publisher = new PublisherImpl(ConfigFactory.empty(), messenger, "run1", "pipeline1");

    Config config = ConfigFactory.parseResourcesAnySyntax("ParquetConnectorTest/conf/fakeFile.conf");
    ParquetConnector connector = new ParquetConnector(config);

    assertThrows(ConnectorException.class, () -> connector.execute(publisher));
  }
}
