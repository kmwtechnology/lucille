package com.kmwllc.lucille.connector;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.DocumentException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.fileHandler.XMLFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Reads data from an RSS feed, and publishes Documents with each RSS item's content in the "rss_item" field on the Document.
 *
 * Config Parameters:
 *
 * rssURL (String): The URL to the RSS feed.
 *
 * encoding (String, Optional): The encoding used in the RSS feed. Defaults to utf-8 encoding.
 * idField (String, Optional): The name of the field in the XML items that serves as an identifier. Defaults to use guid. It is
 * recommended you only override this value if working with an RSS feed which doesn't include a guid for items.
 *
 * <b>Note: The docIdPrefix you specify in your Config will be applied to each of the extracted RSS items.</b>
 */
public class RSSConnector extends AbstractConnector {

  private final URL rssURL;
  private static final XmlMapper xmlMapper = new XmlMapper();

  public RSSConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("rssURL")
        .withOptionalProperties("encoding", "idField"));

    try {
      this.rssURL = new URL(config.getString("rssURL"));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Attempted to create RSSConnector with malformed rssURL.", e);
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      InputStream rssStream = rssURL.openStream();
      JsonNode rssRootNode = xmlMapper.readTree(rssStream);

      // Gets the item(s). Will be an array, if multiple, or just the one object, if only one.
      JsonNode allItemsNode = rssRootNode.get("channel").get("item");

      if (allItemsNode.isArray()) {
        for (JsonNode itemNode : allItemsNode) {
          Document doc = docFromNodeUsingId(itemNode);
          publisher.publish(doc);
        }
      } else if (allItemsNode.isObject()) {
        Document doc = docFromNodeUsingId(allItemsNode);
        publisher.publish(doc);
      } else {
        throw new ConnectorException("The XML's \"item\" node was of incompatible type: " + allItemsNode.getNodeType());
      }
    } catch (IOException e) {
      throw new ConnectorException("Error occurred connecting to the RSS feed:", e);
    } catch (DocumentException e) {
      throw new ConnectorException("Error occurred getting Document from RSS item:", e);
    } catch (Exception e) {
      throw new ConnectorException("Error occurred in RSSConnector:", e);
    }
  }

  private Document docFromNodeUsingId(JsonNode itemNode) {
    Document doc = Document.create(itemNode.get("guid").asText());
    Iterator<Entry<String, JsonNode>> fields = itemNode.fields();

    while (fields.hasNext()) {
      Entry<String, JsonNode> field = fields.next();
      doc.setField(field.getKey(), field.getValue());
    }

    return doc;
  }
}
