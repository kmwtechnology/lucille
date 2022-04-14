package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Stack;
import java.util.UUID;

public class ChunkingXMLHandler implements ContentHandler {

  private static final Logger log = LoggerFactory.getLogger(ChunkingXMLHandler.class);

  Stack<String> currentPath = new Stack<String>();
  private AbstractConnector connector;
  private Publisher publisher;
  private String documentRootPath;
  private String documentIDPath;
  private String docIDPrefix = "";
  private boolean inDocID = false;
  private StringBuilder docIDBuilder = new StringBuilder();
  private RecordingInputStream ris;

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
      docIDBuilder = new StringBuilder();
      try {
        ris.clearUpTo("<" + qName);
      } catch (IOException e) {
        log.error("IOException caught", e);
      }

    }
    if (documentIDPath.equals(path)) {
      // this is the start of the document id field.
      inDocID = true;
    }
  }

  @Override
  public void endElement(String uri, String localName, String qName) throws SAXException {
    // we just finished a path. see if it's the doc root that we're looking for.
    String path = "/" + StringUtils.join(currentPath.toArray(), "/");
    if (documentRootPath.equals(path)) {
      String xml = "Malformed";
      try {
        xml = ris.returnUpTo("</" + qName + ">");
      } catch (IOException e) {
        log.error("IOException caught", e);
      }

      // XPath not supported where id is in attribute (e.g. <field name="id">
      // Quick workaround is to use a UUID instead and assign ID elsewhere in workflow
      String id = docIDBuilder.toString();
      if (id == null || id.equals("")) {
        id = UUID.randomUUID().toString();
      }

      Document doc = new Document(docIDPrefix + id);
      doc.setField("xml", xml);
      internalPublishDocument(doc);
    }
    if (documentIDPath.equals(path)) {
      // this is the end of the doc id tag.
      inDocID = false;
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
    if (inDocID) {
      docIDBuilder.append(Arrays.copyOfRange(ch, start, start + length));
    }
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

  public void setDocumentIDPath(String documentIDPath) {
    this.documentIDPath = documentIDPath;
  }

  public String getDocIDPrefix() {
    return docIDPrefix;
  }

  public void setDocIDPrefix(String docIDPrefix) {
    this.docIDPrefix = docIDPrefix;
  }

  public void setConnector(AbstractConnector connector) {
    this.connector = connector;
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

}