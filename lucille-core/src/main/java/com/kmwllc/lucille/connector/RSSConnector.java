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
import com.kmwllc.lucille.core.Spec;
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
import sun.misc.Signal;

/**
 * <p> Reads the item(s) from an RSS feed, and publishes Documents with each RSS item's content in the "rss_item" field on the Document.
 * Can be configured to only publish items with a recent &lt;pubDate&gt;.
 *
 * <p> Config Parameters:
 *
 * <p> rssURL (String): The URL to the RSS feed you want to publish Documents for.
 * <p> useGuidForDocID (Boolean, Optional): Whether you want the GUID of RSS Items to be the IDs for their corresponding Documents. Defaults
 * to true. If set to false, UUIDs are created for each Document.
 * (&lt;guid&gt;, for example) Defaults to using a UUID for each document. If the field has an attribute, Lucille handles extracting the actual
 * value for your Document's ID.
 * <p> pubDateCutoff (Duration, Optional): Specify a duration to only publish Documents for recent RSS items. For example, "1h" means
 * only RSS items with a &lt;pubDate&gt; within the last hour will be published as Documents. Defaults to including all files.
 * If &lt;pubDate&gt; is not found in the items, this value has no effect.
 *
 * <p> refreshIncrement (Duration, Optional): Specify the frequency with which the Connector should check for updates from the RSS
 * feed. Defaults to no updates (the Connector will run once). When used, the Connector will not publish Documents for the same RSS item multiple times.
 *
 * <p>See the HOCON documentation for examples of a duration.</p>
 *
 * <p> <b>Note:</b> By default, XML elements with attributes (ex: &lt;guid isPermaLink="false"&gt;) will be put onto Documents as a JSON object,
 * keyed by the element name, with both the attribute(s) and the content of the element as child properties. The content's key
 * will be an empty String.
 *
 * <p> Document Fields (All Optional):
 * <ul>
 *   <li><code>author</code> (String)</li>
 *   <li><code>categories</code> (List&lt;String&gt;)</li>
 *   <li><code>comments</code> (List&lt;String&gt;)</li>
 *   <li><code>content</code> (String)</li>
 *   <li><code>description</code> (String)</li>
 *   <li><code>enclosures</code> (List&lt;JsonNode&gt;). Each JsonNode <i>will</i> have <code>type</code> (String) and <code>url</code> (String), and <i>may</i> have "length" (Long).</li>
 *   <li><code>guid</code> (String)</li>
 *   <li><code>isPermaLink</code> (String)</li>
 *   <li><code>link</code> (String)</li>
 *   <li><code>title</code> (String)</li>
 *   <li><code>pubDate</code> (Instant)</li>
 * </ul>
 */
public class RSSConnector extends AbstractConnector {

  private final RssReader rssReader = new RssReader();
  private static final ObjectMapper mapper = new ObjectMapper();
  private static final Logger log = LoggerFactory.getLogger(RSSConnector.class);

  private final boolean useGuidForDocID;
  private final URL rssURL;

  private final Duration pubDateCutoffDuration;

  private final Duration refreshIncrement;

  private boolean running = true;

  private Set<Item> processedItems = new HashSet<>();

  public RSSConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("rssURL")
        .withOptionalProperties("useGuidForDocID", "pubDateCutoff", "refreshIncrement"));

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

    this.refreshIncrement = config.hasPath("refreshIncrement") ? config.getDuration("refreshIncrement") : null;

    Signal.handle(
        new Signal("INT"),
        signal -> {
          this.running = false;
          log.info("RSSConnector to shut down...");
        });
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    do {
      // resetting this cutoff at the start of each iteration, in case it is incremental
      Optional<Instant> pubDateCutoff = Optional.ofNullable(pubDateCutoffDuration == null ? null : Instant.now().minus(pubDateCutoffDuration));

      Set<Item> itemsProcessedThisRefresh = new HashSet<>();

      try (Stream<Item> itemStream = rssReader.read(rssURL.toString())) {
        List<Item> rssItems = itemStream.toList();

        for (Item item : rssItems) {
          // want to add all items retrieved to the set at the end of the iteration
          itemsProcessedThisRefresh.add(item);

          // will allow the item to be published if the pubDateCutoff is null or the item doesn't have a pubDate.
          if (optionalPubDateBeforeCutoff(item, pubDateCutoff) || processedItems.contains(item)) {
            continue;
          }

          Document doc = docFromRSSItem(item);
          publisher.publish(doc);
        }
      } catch (IOException e) {
        throw new ConnectorException("Error occurred connecting to the RSS feed:", e);
      } catch (Exception e) {
        throw new ConnectorException("Error occurred with RSSConnector", e);
      }

      if (refreshIncrement == null) {
        return;
      }

      // Continuously updating it based on what was seen in the iteration to prevent unbounded growth of the set...
      // Luckily we don't have to handle some weird edge case where pubDateCutoff + incremental is used. If something doesn't
      // meet the cutoff 30 seconds ago, it's not going to suddenly be "more recent" 30 seconds later...
      processedItems = itemsProcessedThisRefresh;

      try {
        log.info("Finished checking RSS feed. Will wait to refresh.");
        Instant wakeupInstant = Instant.now().plus(refreshIncrement);

        while (Instant.now().isBefore(wakeupInstant) && running) {
          Thread.sleep(250);
        }

        log.info("RSSConnector to check for new content.");
      } catch (InterruptedException e) {
        throw new ConnectorException("RSSConnector interrupted while waiting to refresh.", e);
      }
    } while (running);
  }

  private Document docFromRSSItem(Item item) {
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

  private static boolean optionalPubDateBeforeCutoff(Item item, Optional<Instant> cutoff) {
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
