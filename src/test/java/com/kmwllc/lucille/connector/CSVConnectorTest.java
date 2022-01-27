package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigResolveOptions;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CSVConnectorTest {

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

}
