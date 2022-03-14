package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.w3c.dom.Element;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


import javax.xml.XMLConstants;
import javax.xml.parsers.*;
import java.io.*;

import java.util.List;
import java.util.regex.Pattern;

/**
 * A connector to parse XML and place all the characteristics of the elements as fields of the document.
 * Currently, this connector treats all fields as Strings.
 */
public class DOMConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(DOMConnector.class);

  private String filePath;
  private String xmlRootPath;
  // a boolean value to set whether the XML element contains the ID or not
  private boolean xmlIDAttribute;
  private String docIDPrefix;


  public DOMConnector(Config config) {
    super(config);
    filePath = config.hasPath("filePath") ? config.getString("filePath") : null;
    xmlRootPath = config.hasPath("xmlRootPath") ? config.getString("xmlRootPath") : null;
    xmlIDAttribute = config.hasPath("xmlIDAttribute") ? config.getBoolean("xmlIDAttribute") : false;
    docIDPrefix = config.hasPath("docIDPrefix") ? config.getString("docIDPrefix") : "doc_";
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    org.w3c.dom.Document doc = null;

    try {
      dbf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);

      DocumentBuilder db = dbf.newDocumentBuilder();
      doc = db.parse(new File(filePath));

    } catch (Exception e) {
      log.error("DOMConnector setup failed", e);
    }

    doc.getDocumentElement().normalize();

    NodeList elements = doc.getElementsByTagName(xmlRootPath);
    if (elements.getLength() == 0) {
      log.warn("No elements found using root path {}", xmlRootPath);
    }

    for (int i = 0; i < elements.getLength(); i++) {
      Node node = elements.item(i);
      if (node.getNodeType() == Node.ELEMENT_NODE) {

        Element element = (Element) node;

        String id = null;
        if (xmlIDAttribute) {
          id = element.getAttribute("id");
        }

        NodeList list = element.getChildNodes();

        Document d = new Document(docIDPrefix + id);
        for (int y = 0; y < list.getLength(); y++) {
          Node n = list.item(y);
          String field = n.getNodeName();
          String val = n.getTextContent();
          d.setField(field, val);
        }
        try {
          publisher.publish(d);
        } catch (Exception e) {
          log.warn("Document " + d + " was unable to be published", e);
        }
      }
    }
  }
}