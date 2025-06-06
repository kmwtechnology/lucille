package com.kmwllc.lucille.connector;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;

import com.apptasticsoftware.rssreader.DateTime;
import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValueFactory;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;
import sun.misc.Signal;

public class RSSConnectorTest {
  private static DateTime parser = new DateTime();

  private Item[] basicItems;
  private Item[] singleItem;
  private Item[] withOld;

  @Before
  public void setup() {
    Item business1 = new Item(parser);
    Item business2 = new Item(parser);
    Item business3 = new Item(parser);

    business1.setGuid("amu042125");
    business1.setTitle("Afternoon Market Update - April 21, 2025");
    business1.setDescription("The Dow is down more than 1,000 points. All indices are lower by 3%+.");
    business1.setPubDate("Mon, 21 Apr 2025 19:30:00 -0600");

    business2.setGuid("uber-news042125");
    business2.setTitle("BREAKING: UBER SUED BY FTC");
    business2.setPubDate("Mon, 21 Apr 2025 17:10:30 -0600");

    business3.setGuid("lei042125");
    business3.setTitle("Leading Economic Indicators - April 21, 2025");
    business3.setPubDate("Mon, 21 Apr 2025 12:00:05 -0600");

    Item old1 = new Item(parser);
    Item old2 = new Item(parser);
    Item old3 = new Item(parser);

    old1.setGuid("mc031620");
    old1.setTitle("Dow drops 3000 points as Fed slashes rates");
    // bad formatting for the PubDate - the RSS library is smart enough to handle this.
    old1.setPubDate("2020-03-16T21:30:00Z");
    old1.setDescription("The Dow lost 2,997 points, or 12.3%. Circuit breakers were triggered twice.");

    // old story, no pubdate
    old2.setGuid("mc101308");
    old2.setTitle("Stocks Surge as Central Banks Stimulate");
    old2.setDescription("The Dow gained a record 936 points as central banks attempted to save the economy.");

    // old story, correct pubDate formatting
    old3.setGuid("mc091508");
    old3.setTitle("Stocks Tank as Financials Implode");
    old3.setPubDate("Mon, 15 Sep 2008 16:01:00 GMT");
    old3.setDescription("The Dow lost 504 points (4.4%) as Lehman Brothers filed for bankruptcy.");

    basicItems = new Item[] { business1, business2, business3 };
    singleItem = new Item[] { business1 };
    withOld = new Item[] { business1, business2, business3, old1, old2, old3 };
  }

  @Test
  public void testRSSConnector() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class))).thenReturn(Stream.of(basicItems));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());

    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
    // checking here that the formatting around the description (in the XML file) gets removed...
    assertEquals("The Dow is down more than 1,000 points. All indices are lower by 3%+.", publishedDocs.get(0).getString("description"));
    assertEquals(Set.of("guid", "description", "title", "pubDate", "run_id", "id"), publishedDocs.get(0).getFieldNames());

    assertEquals("uber-news042125", publishedDocs.get(1).getId());
    assertEquals("BREAKING: UBER SUED BY FTC", publishedDocs.get(1).getString("title"));
    assertEquals(Set.of("guid", "title", "pubDate", "run_id", "id"), publishedDocs.get(1).getFieldNames());

    assertEquals("lei042125", publishedDocs.get(2).getId());
    assertEquals("Leading Economic Indicators - April 21, 2025", publishedDocs.get(2).getString("title"));
    assertEquals(Set.of("guid", "title", "pubDate", "run_id", "id"), publishedDocs.get(2).getFieldNames());
  }

  @Test
  public void testRSSConnectorSingleItem() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class))).thenReturn(Stream.of(singleItem));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(1, publishedDocs.size());
    assertEquals("amu042125", publishedDocs.get(0).getId());
    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
  }

  @Test
  public void testRSSConnectorWithIdField() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/noUseGuid.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    RSSConnector connector;
    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class))).thenReturn(Stream.of(basicItems));
    })) {
      connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());

    // Making sure we can get UUIDs from each of the docIDs
    UUID.fromString(publishedDocs.get(0).getId());
    UUID.fromString(publishedDocs.get(1).getId());
    UUID.fromString(publishedDocs.get(2).getId());
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

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class))).thenReturn(Stream.of(withOld));
    })) {
      RSSConnector connector = new RSSConnector(config);
      connector.execute(publisher);
    }

    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(4, publishedDocs.size());

    assertEquals("Afternoon Market Update - April 21, 2025", publishedDocs.get(0).getString("title"));
    assertEquals("BREAKING: UBER SUED BY FTC", publishedDocs.get(1).getString("title"));
    assertEquals("Leading Economic Indicators - April 21, 2025", publishedDocs.get(2).getString("title"));
    // has no pubDate, so still gets published
    assertEquals("Stocks Surge as Central Banks Stimulate", publishedDocs.get(3).getString("title"));
  }

  @Test
  public void testRSSConnectorIncremental() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/incremental.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class)))
          .thenReturn(Stream.of(singleItem))
          .thenReturn(Stream.of(basicItems));
    })) {
      RSSConnector connector = new RSSConnector(config);

      // Stop the connector after 2 seconds, allowing it to run twice, and then it gets interrupted.
      scheduler.schedule(() -> {
        Signal.raise(new Signal("INT"));
      }, 2, TimeUnit.SECONDS);

      // blocked until the signal gets raised. should run 2 times
      connector.execute(publisher);
    }

    // Should run 2 times: 0 sec, 1.2 sec, then interrupted.
    // Only has 3 unique items to be published
    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(3, publishedDocs.size());
  }

  @Test
  public void testIncrementalAndCutoff() throws Exception {
    LocalDate start = LocalDate.of(2025, 1, 1);
    LocalDate today = LocalDate.now();
    long daysSince2025Start = ChronoUnit.DAYS.between(start, today);
    String durationStr = daysSince2025Start + "d";

    Config config = ConfigFactory
        .parseResourcesAnySyntax("RSSConnectorTest/incremental.conf")
        .withValue("pubDateCutoff", ConfigValueFactory.fromAnyRef(durationStr));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read(any(String.class)))
          // simulating a "real" RSS experience - older content slowly getting removed from the feed
          .thenReturn(Stream.of(withOld))
          .thenReturn(Stream.of(basicItems))
          .thenReturn(Stream.of(singleItem));
    })) {
      RSSConnector connector = new RSSConnector(config);

      // Stop the connector after 3 seconds, allowing it to run three times, and then is interrupted.
      scheduler.schedule(() -> {
        Signal.raise(new Signal("INT"));
      }, 3, TimeUnit.SECONDS);

      // will be blocked until the signal gets raised. should run three times
      connector.execute(publisher);
    }

    // Should be run 3 times, only 4 items w/ valid (or missing) pubDates to be published
    List<Document> publishedDocs = messenger.getDocsSentForProcessing();
    assertEquals(4, publishedDocs.size());
  }

  @Test
  public void testRSSConnectorBadUrl() {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/badURL.conf");
    assertThrows(IllegalArgumentException.class, () -> new RSSConnector(config));
  }

  @Test
  public void testRSSConnectorBadURLConnection() throws Exception {
    Config config = ConfigFactory.parseResourcesAnySyntax("RSSConnectorTest/default.conf");

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "xmlRun", "xmlPipeline");

    try (MockedConstruction<RssReader> mockReaderConst = Mockito.mockConstruction(RssReader.class, (mockReader, context) -> {
      when(mockReader.read("https://www.businessNews.com/rss/rss.html")).thenThrow(new IOException("failed to connect to URL"));
    })) {
      RSSConnector connector = new RSSConnector(config);
      assertThrows(ConnectorException.class, () -> connector.execute(publisher));
    }
  }
}
