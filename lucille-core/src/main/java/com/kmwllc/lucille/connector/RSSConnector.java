package com.kmwllc.lucille.connector;

import com.apptasticsoftware.rssreader.Enclosure;
import com.apptasticsoftware.rssreader.Item;
import com.apptasticsoftware.rssreader.RssReader;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads items from an RSS feed and publishes a Document for each item. Each document includes the item's fields:
 * author (String), categories (List&lt;String&gt;), comments (String), content (String), description (String),
 * enclosures (List&lt;ObjectNode&gt;: {type:String, url:String, length?:Long})), guid (String), isPermaLink (Boolean),
 * link (String), title (String), pubDate (Instant). Each document stores the item's payload under
 * the rss_item field. Can be configured to publish only items with a recent pubDate. Supports incremental runs
 * via runDuration and refreshIncrement. Durations use HOCON-style strings such as "1h", "2d", and "3s".
 * <p>
 * Config Parameters -
 * <ul>
 *   <li>rssURL (String, Required) : URL of the RSS feed.</li>
 *   <li>useGuidForDocID (Boolean, Optional) : Use the RSS item GUID as the Document ID. Defaults to true; if false, a UUID is generated per Document.</li>
 *   <li>pubDateCutoff (String, Optional) : Duration string; only publish items with a pubDate within this period. Defaults to including all items.
 *   If the pubDate is missing, the item will be included.</li>
 *   <li>runDuration (String, Optional) : Total time to run when refreshing incrementally; must be used with refreshIncrement.</li>
 *   <li>refreshIncrement (String, Optional) : Interval between feed refreshes; must be used with runDuration.</li>
 * </ul>
 */
public class RSSConnector extends AbstractConnector {

  public static final Spec SPEC = SpecBuilder.connector()
      .requiredString("rssURL")
      .optionalBoolean("useGuidForDocID")
      // best to reference durations as Strings
      .optionalString("pubDateCutoff", "runDuration", "refreshIncrement").build();

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger log = LoggerFactory.getLogger(RSSConnector.class);

  private final URL rssURL;
  private final boolean useGuidForDocID;
  private final Duration pubDateCutoffDuration;

  private final Duration runDuration;
  private final Duration refreshIncrement;
  private Set<Item> itemsProcessedLastRefresh = new HashSet<>();

  private final RssReader rssReader = new RssReader();

  public RSSConnector(Config config) {
    super(config);

    try {
      this.rssURL = new URL(config.getString("rssURL"));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Attempted to create RSSConnector with malformed rssURL.", e);
    }

    this.useGuidForDocID = ConfigUtils.getOrDefault(config, "useGuidForDocID", true);

    if (config.hasPath("pubDateCutoff")) {
      this.pubDateCutoffDuration = config.getDuration("pubDateCutoff");
    } else {
      this.pubDateCutoffDuration = null;
    }

    if (config.hasPath("runDuration") != config.hasPath("refreshIncrement")) {
      throw new IllegalArgumentException("runDuration and refreshIncrement must both be defined to run incrementally.");
    }

    this.runDuration = config.hasPath("runDuration") ? config.getDuration("runDuration") : null;
    this.refreshIncrement = config.hasPath("refreshIncrement") ? config.getDuration("refreshIncrement") : null;
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    Instant executionStarted = Instant.now();

    do {
      // resetting this cutoff at the start of each iteration, in case it is incremental
      Optional<Instant> pubDateCutoff = Optional.ofNullable(pubDateCutoffDuration == null ? null : Instant.now().minus(pubDateCutoffDuration));

      Set<Item> itemsProcessedThisRefresh = new HashSet<>();

      try (Stream<Item> itemStream = rssReader.read(rssURL.toString())) {
        List<Item> rssItems = itemStream.toList();

        for (Item item : rssItems) {
          // want to add all items retrieved to the set at the end of the RSS "refresh"
          itemsProcessedThisRefresh.add(item);

          // method returns false if the pubDateCutoff is empty or the item doesn't have a pubDate.
          if (isPubDateBeforeCutoff(item, pubDateCutoff) || itemsProcessedLastRefresh.contains(item)) {
            continue;
          }

          Document doc = createDoc(item);
          publisher.publish(doc);
        }
      } catch (IOException e) {
        throw new ConnectorException("Error occurred connecting to the RSS feed:", e);
      } catch (Exception e) {
        throw new ConnectorException("Error occurred with RSSConnector", e);
      }

      if (refreshIncrement == null || runDuration == null) {
        return;
      } else if (Instant.now().isAfter(executionStarted.plus(runDuration))) {
        // if we've been running for longer than the runDuration (after this refresh), stop.
        return;
      }

      // Continuously updating it based on what was seen in the iteration to prevent unbounded growth of the set...
      // Luckily we don't have to handle some weird edge case where pubDateCutoff + incremental is used. If something doesn't
      // meet the cutoff 30 seconds ago, it's not going to suddenly be "more recent" 30 seconds later...
      // We do check that it's not empty, however, to prevent one bad "fetch" from causing extra Documents from being published.
      if (!itemsProcessedThisRefresh.isEmpty()) {
        itemsProcessedLastRefresh = itemsProcessedThisRefresh;
      }

      try {
        log.info("Finished checking RSS feed. Will wait to refresh.");
        Instant wakeupInstant = Instant.now().plus(refreshIncrement);

        while (Instant.now().isBefore(wakeupInstant)) {
          Thread.sleep(250);
        }

        log.info("RSSConnector to check for new content.");
      } catch (InterruptedException e) {
        log.info("RSSConnector interrupted, will stop running.");
        return;
      }
    } while (true);
  }

  private Document createDoc(Item item) {
    Document doc;
    if (useGuidForDocID) {
      if (item.getGuid().isPresent()) {
        doc = Document.create(item.getGuid().get());
      } else {
        log.warn("RSSConnector is configured to useGuid, but found an item without one. Using a UUID for docID.");
        doc = Document.create(UUID.randomUUID().toString());
      }
    } else {
      doc = Document.create(UUID.randomUUID().toString());
    }

    item.getAuthor().ifPresent(author -> doc.setField("author", author));
    // note that this is a List, and is non-optional. but the field won't be on a Document if it's empty.
    item.getCategories().forEach(category -> doc.addToField("categories", category));
    item.getComments().ifPresent(comments -> doc.setField("comments", comments));
    item.getContent().ifPresent(content -> doc.setField("content", content));
    item.getDescription().ifPresent(description -> doc.setField("description", description));

    // Adding a JSON for each enclosure to "enclosures" (if there are any)
    if (!item.getEnclosures().isEmpty()) {
      for (Enclosure enc : item.getEnclosures()) {
        ObjectNode encAsNode = mapper.createObjectNode()
            .put("type", enc.getType())
            .put("url", enc.getUrl());

        enc.getLength().ifPresent(length -> encAsNode.put("length", length));

        doc.addToField("enclosures", encAsNode);
      }
    }

    item.getGuid().ifPresent(guid -> doc.setField("guid", guid));
    item.getIsPermaLink().ifPresent(isPermaLink -> doc.setField("isPermaLink", isPermaLink));
    item.getLink().ifPresent(link -> doc.setField("link", link));
    item.getTitle().ifPresent(title -> doc.setField("title", title));
    item.getPubDateZonedDateTime().ifPresent(time -> doc.setField("pubDate", time.toInstant()));

    return doc;
  }

  /** If the item has no pubDate OR the cutoff is empty, returns false - meaning the item should get a Document published.. */
  private static boolean isPubDateBeforeCutoff(Item item, Optional<Instant> cutoff) {
    if (cutoff.isEmpty()) {
      return false;
    }

    if (item.getPubDateZonedDateTime().isEmpty()) {
      log.warn("pubDateCutoff was specified, but encountered an item without a pubDate - it will be published.");
      return false;
    }

    Instant cutoffInstant = cutoff.get();
    Instant pubInstant = item.getPubDateZonedDateTime().get().toInstant();

    return pubInstant.isBefore(cutoffInstant);
  }
}
