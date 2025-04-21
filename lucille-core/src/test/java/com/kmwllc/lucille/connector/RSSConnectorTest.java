package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.FileInputStream;
import java.net.URL;
import java.util.List;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

public class RSSConnectorTest {

  @Test
  public void testRSSConnector() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/config.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here, for some reason...
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    RSSConnector connector;
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNews.xml"));
    })) {
      connector = new RSSConnector(config);
    }

    connector.execute(publisher);
    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());

    // defaults to using the item's guid as the id for the document. it'll still be a separate field on the document, however...
    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("amu042125", publishedDocs.get(0).getString("guid"));
    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
    // note here that the formatting around the description (in the XML file) gets removed...
    assertEquals("The Dow is down more than 1,000 points. All indices are lower by 3%+.", publishedDocs.get(0).getString("description"));

    assertEquals("uber-news042125", publishedDocs.get(1).getId());
    assertEquals("uber-news042125", publishedDocs.get(1).getString("guid"));
    assertEquals("BREAKING: UBER SUED BY FTC", publishedDocs.get(1).getString("title"));

    assertEquals("lei042125", publishedDocs.get(2).getId());
    assertEquals("lei042125", publishedDocs.get(2).getString("guid"));
    assertEquals("Leading Economic Indicators - April 21, 2025", publishedDocs.get(2).getString("title"));
  }

  @Test
  public void testRSSConnectorSingleItem() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/config.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here, for some reason...
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    RSSConnector connector;
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNewsSingle.xml"));
    })) {
      connector = new RSSConnector(config);
    }

    connector.execute(publisher);
    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(1, publishedDocs.size());
    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("amu042125", publishedDocs.get(0).getString("guid"));
    assertEquals("Market Close - April 21, 2025", publishedDocs.get(0).getString("title"));
  }
}
