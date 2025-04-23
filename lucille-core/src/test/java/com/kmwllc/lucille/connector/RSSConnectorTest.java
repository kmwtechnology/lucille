package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URL;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import sun.misc.Signal;

public class RSSConnectorTest {

  @Test
  public void testRSSConnector() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here - some kind of interference from Mockito, I think.
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNews.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());

    assertEquals("amu042125", publishedDocs.get(0).getJson("guid").get("").asText());
    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
    // checking here that the formatting around the description (in the XML file) gets removed...
    assertEquals("The Dow is down more than 1,000 points. All indices are lower by 3%+.", publishedDocs.get(0).getString("description"));

    assertEquals("uber-news042125", publishedDocs.get(1).getJson("guid").get("").asText());
    assertEquals("BREAKING: UBER SUED BY FTC", publishedDocs.get(1).getString("title"));

    assertEquals("lei042125", publishedDocs.get(2).getJson("guid").get("").asText());
    assertEquals("Leading Economic Indicators - April 21, 2025", publishedDocs.get(2).getString("title"));
  }

  @Test
  public void testRSSConnectorSingleItem() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNewsSingle.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(1, publishedDocs.size());
    assertEquals("amu042125", publishedDocs.get(0).getJson("guid").get("").asText());
    assertEquals("Market Close - April 21, 2025", publishedDocs.get(0).getString("title"));
  }

  @Test
  public void testRSSConnectorWithIdField() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/guidForId.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    RSSConnector connector;
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNews.xml"));
    })) {
      connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());
    // the guid should be used as the id. Also, the "isPermaLink" attribute won't have any effect.
    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("uber-news042125", publishedDocs.get(1).getId());
    assertEquals("lei042125", publishedDocs.get(2).getId());
  }

  @Test
  public void testRSSConnectorWithPubDateCutoff() throws Exception {
    // Adding this programmatically so the test will succeed forever. There's three "stories" from 2025,
    // and then three coming from before - only one has an actual, correctly formatted pubDate though.
    LocalDate start = LocalDate.of(2025, 1, 1);
    LocalDate today = LocalDate.now();
    long daysSince2025Start = ChronoUnit.DAYS.between(start, today);
    String durationStr = daysSince2025Start + "d";

    Config config = ConfigFactory
        .parseResourcesAnySyntax("RSSConnectorTest/default.conf")
        .withValue("pubDateCutoff", ConfigValueFactory.fromAnyRef(durationStr));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNewsWithOld.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(5, publishedDocs.size());

    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
    assertEquals("BREAKING: UBER SUED BY FTC", publishedDocs.get(1).getString("title"));
    assertEquals("Leading Economic Indicators - April 21, 2025", publishedDocs.get(2).getString("title"));
    // has a badly formatted pubDate, so still gets published
    assertEquals("Dow drops 3000 points as Fed slashes rates", publishedDocs.get(3).getString("title"));
    // has no pubDate, so still gets published
    assertEquals("Stocks Surge as Central Banks Stimulate", publishedDocs.get(4).getString("title"));
  }

  @Test
  public void testRSSConnectorWithNoGuidAttribute() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/guidForId.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    RSSConnector connector;
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      // A bit of a simpler case - <guid> doesn't have isPermaLink. Want to make sure it becomes the docID the same as before.
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/businessNewsNoGuidAttribute.xml"));
    })) {
      connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());
    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("uber-news042125", publishedDocs.get(1).getId());
    assertEquals("lei042125", publishedDocs.get(2).getId());
  }

  @Test
  public void testRSSConnectorBadUrl() {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/badURL.conf");
    assertThrows(IllegalArgumentException.class, () -> new RSSConnector(config));
  }

  @Test
  public void testRSSConnectorIncompatibleItemNodeType() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here, for some reason...
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/weird.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    }
  }

  @Test
  public void testRSSConnectorBadURLConnection() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here, for some reason...
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenThrow(new IOException("mock IO Exc."));
    })) {
      RSSConnector connector = new RSSConnector(config);
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    }
  }

  // Making sure the connector can read "RDF" RSS feeds - this is a "1.*" version of RSS. The "business news" example
  // was more in line with "Harvard" / version "2.*".
  @Test
  public void testRSSConnectorRDF() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    // The test will fail if we do not reference the class here, for some reason...
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      Mockito.when(mockURL.openStream()).thenReturn(new FileInputStream("src/test/Resources/RSSConnectorTest/rdf.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(1, publishedDocs.size());

    assertEquals("http://www.w3.org/News/2001#item178", publishedDocs.get(0).getString("about"));
  }

  @Test
  public void testRSSConnectorIncremental() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/incremental.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    // Stop the connector after 8 seconds, allowing it to run twice, and then it gets interrupted.
    scheduler.schedule(() -> {
      Signal.raise(new Signal("INT"));
    }, 8, TimeUnit.SECONDS);

    // The test will fail if we do not reference the class here - some kind of interference from Mockito, I think.
    Class.forName("com.kmwllc.lucille.connector.RSSConnector");
    try (MockedConstruction<URL> mockUrlConst = Mockito.mockConstruction(URL.class, (mockURL, context) -> {
      // using thenAnswer so it repeatedly opens FileInputStreams to the file
      Mockito.when(mockURL.openStream()).thenAnswer(invocation -> new FileInputStream("src/test/Resources/RSSConnectorTest/businessNews.xml"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      // will be blocked until the signal gets raised. should run three times
      connector.execute(publisher);
    }

    // Should run 2 times: 0 sec, 5 sec, then interrupted.
    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(6, publishedDocs.size());
  }
}
