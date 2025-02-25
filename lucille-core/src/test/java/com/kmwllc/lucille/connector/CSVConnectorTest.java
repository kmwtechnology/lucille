package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.TestMessenger;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class CSVConnectorTest {

  @Before
  public void setUp() {
    // create a temporary directory to hold copies of the csv
    File tempDir = new File("temp");
    tempDir.mkdirs();
  }


  @Test
  public void testDefaults() throws Exception {
    // we're loading the config this way to prevent the "path" system property from overriding the path
    // property in the connector config; ConfigFactory.parseReader() does not consider system
    // properties like ConfigFactory.load() does
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/defaults.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/config.conf
    // field1, field2, field3
    //  a, b, c
    //  d, "e,f", g
    //  x, y, z

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    System.out.println(docs.get(1));
    assertEquals("e,f", docs.get(1).getString("field2"));
  }

  @Test
  public void testTabsAndNoninterpretedQuotes() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/tabs.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/config.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	"e,f	g
    //  x	y	z

    // verify that tabs takes precedence over specified separator character
    List<Document> docs = messenger.getDocsSentForProcessing();
    System.out.println(docs.get(1));
    assertEquals(3, docs.size());
    assertEquals("\"e,f", docs.get(1).getString("field2"));
  }

  @Test(expected = ConnectorException.class)
  public void testPathNotFound() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/pathNotFound.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);
  }

  @Test
  public void testBOMHandling() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/bom.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of bom.csv (first character is the BOM character \uFEFF)
    // name, price, country
    // Carbonara, 30, Italy
    // Pizza, 10, Italy
    // Tofu Soup, 12, Korea

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    // retrieve a document from the list and ensure that the first column does not contain the BOM
    assertTrue(docs.get(0).getFieldNames().contains("name"));

    // there should be no issues accessing the field value of the first column because BOM is removed
    assertEquals("Carbonara", docs.get(0).getString("name"));
  }

  @Test
  public void testErrorDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy faulty csv into temp directory
    File copy = new File("src/test/resources/CSVConnectorTest/faulty.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/faulty.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);

    assertThrows(ConnectorException.class, () -> connector.execute(publisher));

    // verify error directory is made
    File errorDir = new File("error");
    File f = new File("error/faulty.csv");

    try {
      // verify error directory is made
      assertTrue(errorDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete all created folders and files
      f.delete();
      errorDir.delete();
    }
  }

  @Test
  public void testSuccessfulDirectory() throws Exception {
    File tempDir = new File("temp");

    // copy successful csv into temp directory
    File copy = new File("src/test/resources/CSVConnectorTest/defaults.csv");
    org.apache.commons.io.FileUtils.copyFileToDirectory(copy, tempDir);

    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/success.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);

    connector.execute(publisher);

    // verify error directory is made
    File successDir = new File("success");
    File f = new File("success/defaults.csv");

    try {
      // verify error directory is made
      assertTrue(successDir.exists());
      // verify file is moved inside error directory
      assertTrue(f.exists());
    } finally {
      // delete all created folders and files
      f.delete();
      successDir.delete();
    }
  }

  @Test
  public void testSemicolonSeparator() throws Exception {
    Config config = ConfigFactory.parseReader(FileContentFetcher.getOneTimeReader("classpath:CSVConnectorTest/semicolons.conf", ConfigFactory.empty()));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/semicolons.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	f	g
    //  x	y	z

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());
    assertEquals("a", docs.get(0).getString("field1"));
  }

  @After
  public void tearDown() {
    File tempDir = new File("temp");
    tempDir.delete();
  }
}
