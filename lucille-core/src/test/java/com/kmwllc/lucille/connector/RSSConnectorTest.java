package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

public class RSSConnectorTest {

  @Test
  public void sandbox() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/config.conf");
    RSSConnector connector = new RSSConnector(config);

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");
    connector.execute(publisher);

    for (Document d : messenger.getDocsSentForProcessing()) {
      System.out.println(d);
    }
  }
}
