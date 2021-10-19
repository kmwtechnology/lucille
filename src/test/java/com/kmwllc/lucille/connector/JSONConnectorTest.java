package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JSONConnectorTest {

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.load("JSONConnectorTest/config.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(manager, "run1", "pipeline1");
    Connector connector = new JSONConnector(config);
    connector.execute(publisher);

    // contents of JSONConnectorTest/config.conf
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();
    assertEquals(3, docs.size());

    // prefix should be applied to doc ids and run_id should be added
    Document doc1 = Document.fromJsonString("{\"id\": \"PREFIX1\", \"field1\":\"val1-1\", \"field2\":[\"val2-1a\", \"val2-1b\"],\"run_id\":\"run1\"}");
    Document doc2 = Document.fromJsonString("{\"id\": \"PREFIX2\", \"field3\":\"val3\", \"field2\":[\"val2-2a\", \"val2-2b\"],\"run_id\":\"run1\"}");
    Document doc3 = Document.fromJsonString("{\"id\": \"PREFIX3\", \"field4\":\"val4\", \"field5\":\"val5\",\"run_id\":\"run1\"}");
    assertEquals(doc1, docs.get(0));
    assertEquals(doc2, docs.get(1));
    assertEquals(doc3, docs.get(2));
  }

}
