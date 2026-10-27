package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class JSONConnectorTest {

  @Rule
  public final TemporaryFolder tempFolder = new TemporaryFolder();

  @Test
  public void testExecute() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("JSONConnectorTest/config.conf");
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new JSONConnector(config);
    connector.execute(publisher);

    // contents of JSONConnectorTest/config.conf
    // {"id": "1", "field1":"val1-1", "field2":["val2-1a", "val2-1b"]}
    // {"id": "2", "field3":"val3", "field2":["val2-2a", "val2-2b"]}
    // {"id": "3", "field4":"val4", "field5":"val5"}

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(3, docs.size());

    // prefix should be applied to doc ids and run_id should be added
    Document doc1 = Document.createFromJson(
        "{\"id\": \"PREFIX1\", \"field1\":\"val1-1\", \"field2\":[\"val2-1a\", \"val2-1b\"],\"run_id\":\"run1\"}");
    Document doc2 = Document.createFromJson(
        "{\"id\": \"PREFIX2\", \"field3\":\"val3\", \"field2\":[\"val2-2a\", \"val2-2b\"],\"run_id\":\"run1\"}");
    Document doc3 = Document.createFromJson("{\"id\": \"PREFIX3\", \"field4\":\"val4\", \"field5\":\"val5\",\"run_id\":\"run1\"}");
    assertEquals(doc1, docs.get(0));
    assertEquals(doc2, docs.get(1));
    assertEquals(doc3, docs.get(2));
  }

  @Test
  public void testMaxRecordBytes() throws Exception {
    File file = tempFolder.newFile("test.jsonl");
    Files.writeString(file.toPath(), "{\"id\": \"1\"}\n{\"id\": \"2\", \"a\": \"this line is too long\"}\n");

    Config config = ConfigFactory.parseMap(Map.of(
        "name", "connector1", "pipeline", "pipeline1", "jsonPath", file.getAbsolutePath(), "maxRecordBytes", 20));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    Connector connector = new JSONConnector(config);

    assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    assertEquals(1, messenger.getDocsSentForProcessing().size());
  }
}
