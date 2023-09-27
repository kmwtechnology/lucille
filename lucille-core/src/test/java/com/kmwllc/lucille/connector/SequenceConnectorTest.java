package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.util.List;
import org.junit.Test;

public class SequenceConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.load("SequenceConnectorTest/config.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new SequenceConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(100, docs.size());

    // prefix should be applied to doc ids and run_id should be added
    Document doc1 = Document.createFromJson(
        "{\"id\": \"PREFIX10000\", \"run_id\": \"run1\"}");
    Document doc2 = Document.createFromJson(
        "{\"id\": \"PREFIX10001\", \"run_id\": \"run1\"}");
    Document doc3 = Document.createFromJson("{\"id\": \"PREFIX10002\", \"run_id\": \"run1\"}");
    Document doc99 = Document.createFromJson("{\"id\": \"PREFIX10099\", \"run_id\": \"run1\"}");
    assertEquals(doc1, docs.get(0));
    assertEquals(doc2, docs.get(1));
    assertEquals(doc3, docs.get(2));
    assertEquals(doc99, docs.get(99));
  }

}
