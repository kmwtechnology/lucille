package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.fileHandler.FileHandlerException;
import com.kmwllc.lucille.core.fileHandler.XMLFileHandler;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

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
  private final XMLFileHandler xmlHandler;

  public RSSConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("rssURL")
        .withOptionalProperties("encoding", "idField"));

    try {
      this.rssURL = new URL(config.getString("rssURL"));
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException("Attempted to create RSSConnector with malformed rssURL.", e);
    }

    // building the appropriate config for the XMLFileHandler based on the desired output + what may have been specified
    // in the config
    Map<String, Object> xmlHandlerConfigMap = new HashMap<>();
    xmlHandlerConfigMap.put("xmlRootPath", "/rss/channel/item");
    xmlHandlerConfigMap.put("outputField", "rss_item");
    xmlHandlerConfigMap.put("docIdPrefix", getDocIdPrefix());

    if (config.hasPath("idField")) {
      xmlHandlerConfigMap.put("xmlIdPath", config.getString("idField"));
    } else {
      xmlHandlerConfigMap.put("xmlIdPath", "guid");
    }

    if (config.hasPath("encoding")) {
      xmlHandlerConfigMap.put("encoding", config.getString("encoding"));
    }

    Config xmlHandlerConfig = ConfigFactory.parseMap(xmlHandlerConfigMap);
    this.xmlHandler = new XMLFileHandler(xmlHandlerConfig);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    try {
      InputStream rssStream = rssURL.openStream();
      xmlHandler.processFileAndPublish(publisher, rssStream, rssURL.toString());
    } catch (IOException e) {
      throw new ConnectorException("Error occurred connecting to the RSS feed:", e);
    } catch (FileHandlerException e) {
      throw new ConnectorException("Error occurred handling the RSS response:", e);
    }
  }
}
