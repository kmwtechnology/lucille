package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.typesafe.config.Config;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class XPathExtractor extends Stage {
  // fields to xpath
  protected Map<String, Object> xpaths;
  private DocumentBuilder builder;
  private DocumentBuilderFactory factory;
  private XPath xpath;
  private final String xmlField;
  private Map<XPathExpression, String> expressionMapping;
  private static final Logger log = LogManager.getLogger(XPathExtractor.class);

  public XPathExtractor(Config config) {
    super(config);
    xpaths = config.getConfig("fieldMapping").root().unwrapped();
    factory = DocumentBuilderFactory.newInstance();
    XPathFactory xpathFactory = XPathFactory.newInstance();
    xpath = xpathFactory.newXPath();
    xmlField = config.hasPath("xmlField") ? config.getString("xmlField") : "xml";
    expressionMapping = new HashMap<>();
  }

  @Override
  public void start() throws StageException {
    try {

      builder = factory.newDocumentBuilder();

      for (String field : xpaths.keySet()) {
        String expressionString = (String) xpaths.get(field);
        XPathExpression xPathExpression = xpath.compile(expressionString);
        expressionMapping.put(xPathExpression, field);
      }

    } catch (ParserConfigurationException | XPathExpressionException e) {
      throw new StageException("XPathExtractor initialization error", e);
    }
  }

  @Override
  public List<Document> processDocument(Document doc) {
    if (!doc.has(xmlField)) {
      // no xml on this document to process for this stage .. skipping
      return null;
    }

    String xml = doc.getString(xmlField);
    processXml(xml, doc);

    return null;
  }

  private void processXml(String xml, Document doc) {
    try (InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8))) {

      org.w3c.dom.Document xmldoc = builder.parse(stream);

      for (XPathExpression expression : expressionMapping.keySet()) {
        NodeList result = (NodeList) expression.evaluate(xmldoc, XPathConstants.NODESET);
        String fieldName = expressionMapping.get(expression);

        for (int i = 0; i < result.getLength(); i++) {
          if (i == 0) {
            doc.setField(fieldName, result.item(i).getTextContent().trim());
          } else {
            doc.addToField(fieldName, result.item(i).getTextContent().trim());
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting xpath for doc {}.", doc.getId());
    }
  }
}