package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.UUID;
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
 * <p> idField (String, Optional): The name of the field in the RSS items that you want to use for the ID of documents published.
 * (&lt;guid&gt;, for example) Defaults to using a UUID for each document. If the field has an attribute, Lucille handles extracting the actual
 * value for your Document's ID.
 * <p> pubDateCutoff (Duration, Optional): Specify a duration to only publish Documents for recent RSS items. For example, "1h" means
 * only RSS items with a &lt;pubDate&gt; within the last hour will be published as Documents. Defaults to including all files.
 * If &lt;pubDate&gt; is not found in the items, this value has no effect.
 * <p> refreshIncrement (Duration, Optional): Specify the frequency with which the Connector should check for updates from the RSS
 * feed. Defaults to no updates (the Connector will run once).
 *
 * <p>See the HOCON documentation for examples of a duration.</p>
 *
 * <p> <b>Note:</b> By default, XML elements with attributes (ex: &lt;guid isPermaLink="false"&gt;) will be put onto Documents as a JSON object,
 * keyed by the element name, with both the attribute(s) and the content of the element as child properties. The content's key
 * will be an empty String.
 */
public class RSSConnector extends AbstractConnector {

  private static final XmlMapper xmlMapper = new XmlMapper();
  private static final Logger log = LoggerFactory.getLogger(RSSConnector.class);

  // The pubDates *should* comply with RFC 822. Four-digit years are more common than two-digit years, but both
  // are allowed.
  private static final SimpleDateFormat PUB_DATE_FORMAT = new SimpleDateFormat("EEE, dd MMM yy HH:mm:ss Z");

  private final URL rssURL;
  private final String idField;
  private final Date pubDateCutoff;

  private Duration refreshIncrement;

  private boolean running = true;

  public RSSConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("rssURL")
        .withOptionalProperties("idField", "pubDateCutoff", "refreshIncrement"));

    try {
      this.rssURL = new URL(config.getString("rssURL"));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Attempted to create RSSConnector with malformed rssURL.", e);
    }

    this.idField = ConfigUtils.getOrDefault(config, "idField", null);

    if (config.hasPath("pubDateCutoff")) {
      Duration pubDateCutoffDuration = config.getDuration("pubDateCutoff");
      this.pubDateCutoff = Date.from(Instant.now().minus(pubDateCutoffDuration));
    } else {
      this.pubDateCutoff = null;
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
      try (InputStream rssStream = rssURL.openStream()) {
        JsonNode rssRootNode = xmlMapper.readTree(rssStream);
        // Gets the item(s). Will be an array, if multiple, or just the one object, if only one.
        JsonNode allItemsNode = rssRootNode.get("channel").get("item");

        if (allItemsNode.isArray()) {
          for (JsonNode itemNode : allItemsNode) {
            createAndPublishDocumentFromNode(itemNode, publisher);
          }
        } else if (allItemsNode.isObject()) {
          createAndPublishDocumentFromNode(allItemsNode, publisher);
        } else {
          throw new ConnectorException("The XML's \"item\" node was of incompatible type: " + allItemsNode.getNodeType());
        }
      } catch (IOException e) {
        throw new ConnectorException("Error occurred connecting to the RSS feed:", e);
      } catch (Exception e) {
        throw new ConnectorException("Error occurred in RSSConnector:", e);
      }

      if (refreshIncrement == null) {
        return;
      }

      try {
        log.info("Finished checking RSS feed. Will wait to refresh.");
        Instant wakeupInstant = Instant.now().plus(refreshIncrement);

        while (Instant.now().isBefore(wakeupInstant) && running) {
          Thread.sleep(250);
        }
      } catch (InterruptedException e) {
        throw new ConnectorException("RSSConnector interrupted while waiting to refresh.", e);
      }
    } while (running);
  }

  // Attempts to create a Document from the given JsonNode - which should represent an RSS item - and publish it,
  // if appropriate.
  private void createAndPublishDocumentFromNode(JsonNode itemNode, Publisher publisher) throws Exception {
    Document doc = docFromNodeUsingId(itemNode);

    // enforcing the cutoff if it is specified and the item has a pubDate. If something fails, just publish the Document.
    if (pubDateCutoff != null && doc.has("pubDate")) {
      try {
        Date pubDate = PUB_DATE_FORMAT.parse(doc.getString("pubDate"));

        if (pubDate.before(pubDateCutoff)) {
          return;
        }
      } catch (ParseException e) {
        log.warn("Error parsing pubDate {} from Document. It will be published.", doc.getString("pubDate"));
      }
    }

    publisher.publish(doc);
  }

  // Creates a Document from the node, using the idField for an ID if it is specified; if not, using a random UUID.
  // Places all fields from the node onto the item as JSON.
  private Document docFromNodeUsingId(JsonNode itemNode) {
    Document doc;

    if (idField == null) {
      doc = Document.create(createDocId(UUID.randomUUID().toString()));
    } else {
      JsonNode idNode = itemNode.get(idField);

      // Handling the case where the field had an attribute (see unit tests)
      if (idNode.isObject()) {
        doc = Document.create(createDocId(idNode.get("").asText()));
      } else {
        doc = Document.create(createDocId(idNode.asText()));
      }
    }

    Iterator<Entry<String, JsonNode>> fields = itemNode.fields();

    while (fields.hasNext()) {
      Entry<String, JsonNode> field = fields.next();

      // the Document could contain elements in a namespace. For example, "<metadata:id>" which
      // will get mapped as just "<id>" by XmlMapper.
      if (Document.RESERVED_FIELDS.contains(field.getKey())) {
        continue;
      }

      doc.setField(field.getKey(), field.getValue());
    }

    return doc;
  }
}
