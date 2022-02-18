package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Stage;
import com.typesafe.config.Config;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.*;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public class XPathExtractor extends Stage {
  // xpaths to field
  protected Map<String, Object> xpaths;
  private DocumentBuilder builder;
  private DocumentBuilderFactory factory;
  private XPathFactory xpathFactory;
  private XPath xpath;

  public XPathExtractor(Config config) throws ParserConfigurationException {
    super(config);
    xpaths = config.getConfig("fieldMapping").root().unwrapped();
    factory = DocumentBuilderFactory.newInstance();
    builder = factory.newDocumentBuilder();
    xpathFactory = XPathFactory.newInstance();
    xpath = xpathFactory.newXPath();
  }

  @Override
  public List<Document> processDocument(Document doc) {

    if (!doc.has("xml")) {
      // no xml on this document to process for this stage .. skipping
      return null;
    }

    String xml = doc.getString("xml");

    try {
      processXml(xml, doc);
    } catch (XPathExpressionException | SAXException | IOException e) {
      System.out.println("XPath error process doc : ");
    }
    return null;
  }


  private void processXml(String xml, Document doc) throws IOException, SAXException, XPathExpressionException {
    InputStream stream = new ByteArrayInputStream(xml.getBytes(StandardCharsets.UTF_8));
    org.w3c.dom.Document xmldoc = builder.parse(stream);

    for (String expressionString : xpaths.keySet()) {
      XPathExpression xPathExpression = xpath.compile(expressionString);

      NodeList result = (NodeList) xPathExpression.evaluate(xmldoc, XPathConstants.NODESET);

      String fieldName = (String) xpaths.get(expressionString);
      for (int i = 0; i < result.getLength(); i++) {
        if (i == 0) {
          doc.setField(fieldName, result.item(i).getTextContent().trim());
        } else {
          doc.addToField(fieldName, result.item(i).getTextContent().trim());
        }
      }
    }
  }
}
