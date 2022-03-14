package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.xml.DOMConnector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;


import java.util.List;

import static org.junit.Assert.assertEquals;

public class DOMConnectorTest {

  @Test
  public void testDOMConnector() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:DOMConnectorTest/staff.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new DOMConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

    assertEquals(2, docs.size());
    assertEquals("{\"id\":\"doc_1001\",\"#text\":\"\\n    " +
      "\",\"name\":\"daniel\",\"role\":\"software engineer\",\"salary\":\"3000\",\"bio\":\"" +
      "I am from San Diego\",\"run_id\":\"run1\"}", docs.get(0).toString());
    assertEquals("{\"id\":\"doc_1002\",\"#text\":\"\\n  " +
      "  \",\"name\":\"brian\",\"role\":\"admin\",\"salary\":\"8000\",\"bio\":" +
      "\"I enjoy reading\",\"run_id\":\"run1\"}", docs.get(1).toString());
  }
}
