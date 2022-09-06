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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Extracts values out of XML document fields using XPath expressions.
 * <p>
 * Config Parameters -
 * <ul>
 * <li>fieldMapping (Map<String, List<String>>) : A mapping of the XPath expression to the list of fields to place the evaluated expression in.</li>
 * <li>xmlField (String) : The name of the document field which contains the XML field: defaults to "xml".</li>
 * </ul>
 */
public class XPathExtractor extends Stage {
  protected Map<String, Object> xpaths;
  private DocumentBuilder builder;
  private DocumentBuilderFactory factory;
  private XPath xpath;
  private final String xmlField;
  private Map<XPathExpression, List<String>> expressionMapping;
  private static final Logger log = LogManager.getLogger(XPathExtractor.class);

  /**
   * Creates an instance of the XPathExtractor Stage with a given configuration.
   *
   * @param config
   */
  public XPathExtractor(Config config) {
    super(config);
    xpaths = config.getConfig("fieldMapping").root().unwrapped();
    factory = DocumentBuilderFactory.newInstance();
    XPathFactory xpathFactory = XPathFactory.newInstance();
    xpath = xpathFactory.newXPath();
    xmlField = config.hasPath("xmlField") ? config.getString("xmlField") : "xml";
    expressionMapping = new HashMap<>();
  }

  /**
   * @throws StageException if XPath expression cannot be compiled.
   */
  @Override
  public void start() throws StageException {
    try {

      builder = factory.newDocumentBuilder();

      for (String expressionString : xpaths.keySet()) {
        XPathExpression xPathExpression = xpath.compile(expressionString);
        List<String> fields = (List<String>) xpaths.get(expressionString);
        expressionMapping.put(xPathExpression, fields);
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

  @Override
  public List<String> getPropertyList() {
    return List.of("fieldMapping");
  }

  private void processXml(String xml, Document doc) {
    try (InputStream stream = new ByteArrayInputStream(xml.getBytes())) {

      org.w3c.dom.Document xmldoc = builder.parse(stream);

      for (XPathExpression expression : expressionMapping.keySet()) {
        NodeList result = (NodeList) expression.evaluate(xmldoc, XPathConstants.NODESET);

        for (String field : expressionMapping.get(expression)) {
          for (int i = 0; i < result.getLength(); i++) {
            doc.setOrAdd(field, result.item(i).getTextContent());
          }
        }
      }
    } catch (Exception e) {
      log.error("Error extracting xpath for doc {}.", doc.getId());
    }
  }
}