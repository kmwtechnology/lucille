package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
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
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/defaults.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/config.conf
    // field1, field2, field3
    //  a, b, c
    //  d, "e,f", g
    //  x, y, z

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());

    System.out.println(docs.get(1));
    assertEquals("e,f", docs.get(1).getString("field2"));
  }

  @Test
  public void testTabsAndNoninterpretedQuotes() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/tabs.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/config.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	"e,f	g
    //  x	y	z

    // verify that tabs takes precedence over specified separator character
    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    System.out.println(docs.get(1));
    assertEquals(3, docs.size());
    assertEquals("\"e,f", docs.get(1).getString("field2"));
  }

  @Test(expected = ConnectorException.class)
  public void testPathNotFound() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/pathNotFound.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);
  }

  @Test
  public void testBOMHandling() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/bom.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of bom.csv (first character is the BOM character \uFEFF)
    // name, price, country
    // Carbonara, 30, Italy
    // Pizza, 10, Italy
    // Tofu Soup, 12, Korea

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
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

    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/faulty.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);

    connector.execute(publisher);

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

    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/success.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
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
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/semicolons.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    // contents of CSVConnectorTest/semicolons.conf
    //  field1	field2	field3
    //  a	b	c
    //  d	f	g
    //  x	y	z

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());
    assertEquals("a", docs.get(0).getString("field1"));
  }

  @Test
  public void testBreaks() throws Exception {

    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:CSVConnectorTest/test_csv_error.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new CSVConnector(config);
    connector.execute(publisher);

    /* contents of CSVConnectorTest/semicolons.conf
    a,b,c          # header
    1,2,3          # correct line
    4,5            # missing field
    6,7,8          # correct line
    9,10,11        # correct line
    "{}",12,"      # missing closing quote
    ",13,14        # missing closing quote
    ",hi,hello     # missing closing quote
     */

    /* Log
    23/10/31 16:38:56 INFO CSVConnector: Beginning to process file classpath:CSVConnectorTest/test_csv_error.csv
    23/10/31 16:38:56 INFO PublisherImpl: First doc published after 79 ms
    23/10/31 16:38:56 WARN CSVConnector: Line 3 of the csv has a different number of columns than columns in the header.
    23/10/31 16:38:56 WARN CSVConnector: Line 6 of the csv has a different number of columns than columns in the header.
    23/10/31 16:38:56 ERROR CSVConnector: Error reading line number 6 of the csv file. Skipping line.
    ... (error trace)
     23/10/31 16:38:56 WARN CSVConnector: Skipped 1 lines starting at line 6 due to errors.
     */

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());
    // todo review the rest of the errors
  }

  @After
  public void tearDown() {
    File tempDir = new File("temp");
    tempDir.delete();
  }
}
