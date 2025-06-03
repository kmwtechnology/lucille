package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.UUID;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.Stack;

public class ChunkingXMLHandler implements ContentHandler {

  private static final Logger log = LoggerFactory.getLogger(ChunkingXMLHandler.class);

  Stack<String> currentPath = new Stack<String>();

  private final XPath xpath;
  private final DocumentBuilder builder;

  private String outputField;
  private Publisher publisher;
  private String documentRootPath;
  private XPathExpression docIdExpression;
  private boolean skipEmptyId;

  private String docIDPrefix = "";
  private RecordingInputStream ris;

  public ChunkingXMLHandler() throws Exception {
    XPathFactory xpathFactory = XPathFactory.newInstance();
    xpath = xpathFactory.newXPath();
    builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
  }

  @Override
  public void setDocumentLocator(Locator locator) {
  }

  @Override
  public void startDocument() throws SAXException {
  }

  @Override
  public void endDocument() throws SAXException {
  }

  @Override
  public void startPrefixMapping(String prefix, String uri) throws SAXException {
  }

  @Override
  public void endPrefixMapping(String prefix) throws SAXException {
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
    // push on the stack.
    currentPath.push(qName);

    String path = "/" + StringUtils.join(currentPath.toArray(), "/");

    if (documentRootPath.equals(path)) {
      // this is the start of our page.
      try {
        ris.clearUpTo("<" + qName);
      } catch (IOException e) {
        log.error("IOException caught", e);
      }
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    // we just finished a path. see if it's the doc root that we're looking for.
    String path = "/" + StringUtils.join(currentPath.toArray(), "/");
    if (documentRootPath.equals(path)) {
      String xml = "Malformed";
      String id = null;

      try {
        xml = ris.returnUpTo("</" + qName + ">");

        // we have the full XML object. run xpath to get the ID on just this XML.
        try (InputStream xmlStream = new ByteArrayInputStream(xml.getBytes())) {
          org.w3c.dom.Document xmlDoc = builder.parse(xmlStream);
          id = docIdExpression.evaluate(xmlDoc.getDocumentElement());
        }
      } catch (XPathExpressionException e) {
        log.warn("Error evaluating docID for xml {}. Document will have a UUID.", xml, e);
      } catch (IOException e) {
        log.error("IOException caught", e);
      }

      // always publish a Document we successfully got *some* ID String for.
      if (id != null && !id.isEmpty()) {
        Document doc = Document.create(docIDPrefix + id);
        doc.setField(outputField, xml);
        internalPublishDocument(doc);
      } else if (!skipEmptyId) {
        // if we didn't get the id from evaluating the docIDExpression, use a random UUID
        id = UUID.randomUUID().toString();

        Document doc = Document.create(docIDPrefix + id);
        doc.setField(outputField, xml);
        internalPublishDocument(doc);
      } else {
        log.info("No DocID extracted for xml {}. Since skipEmptyId = true, no Document will be published.", xml);
      }
    }

    currentPath.pop();
  }

  private void internalPublishDocument(Document doc) {
    try {
      publisher.publish(doc);
    } catch (Exception e) {
      log.error("Document was unable to be published", e);
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
  }

  @Override
  public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
  }

  @Override
  public void processingInstruction(String target, String data) throws SAXException {
  }

  @Override
  public void skippedEntity(String name) throws SAXException {
  }

  public void setDocumentRootPath(String documentRootPath) {
    this.documentRootPath = documentRootPath;
  }

  public void setDocumentIDPath(String documentIDPath) throws XPathExpressionException {
    this.docIdExpression = xpath.compile(documentIDPath);
  }

  public String getDocIDPrefix() {
    return docIDPrefix;
  }

  public void setDocIDPrefix(String docIDPrefix) {
    this.docIDPrefix = docIDPrefix;
  }

  public RecordingInputStream getRis() {
    return ris;
  }

  public void setRis(RecordingInputStream ris) {
    this.ris = ris;
  }

  public void setPublisher(Publisher publisher) {
    this.publisher = publisher;
  }

  public void setOutputField(String outputField) {
    this.outputField = outputField;
  }

  public void setSkipEmptyId(boolean skipEmptyId) {
    this.skipEmptyId = skipEmptyId;
  }
}
