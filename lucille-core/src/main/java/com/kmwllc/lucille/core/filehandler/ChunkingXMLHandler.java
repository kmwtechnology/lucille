package com.kmwllc.lucille.core.filehandler;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.UnaryOperator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
import org.xml.sax.ext.DeclHandler;

import java.io.IOException;
import java.util.Stack;

class ChunkingXMLHandler implements ContentHandler, DeclHandler {

  private static final Logger log = LoggerFactory.getLogger(ChunkingXMLHandler.class);

  private static final Pattern ENTITY_REF = Pattern.compile("&([\\p{L}_:][\\p{L}\\p{N}._:-]*);");

  private final Stack<String> currentPath = new Stack<String>();

  private final XPath xpath;
  private final DocumentBuilder builder;

  private String outputField;
  private Publisher publisher;
  private String documentRootPath;

  private XPathExpression docIdExpression;

  private String docIdPathStr;
  private boolean inDocIdPath = false;
  // only for using a idPathStr, not for xpath
  private StringBuilder docIdBuilder;

  private boolean skipEmptyId;

  // applies the handler's docIdPrefix; see BaseFileHandler.createDocId
  private UnaryOperator<String> docIdCreator = UnaryOperator.identity();
  private RecordingInputStream ris;

  // the file's internal general entities, name to replacement text; only collected when a DOCTYPE is allowed
  private final Map<String, String> entities = new LinkedHashMap<>();

  public ChunkingXMLHandler() throws Exception {
    XPathFactory xpathFactory = XPathFactory.newInstance();
    this.xpath = xpathFactory.newXPath();
    this.builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
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
      if (docIdPathStr != null) {
        // this is the start of our page.
        docIdBuilder = new StringBuilder();
      }

      try {
        ris.clearUpTo("<" + qName);
      } catch (IOException e) {
        log.error("IOException caught", e);
      }
    }

    if (path.equals(docIdPathStr)) {
      inDocIdPath = true;
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
        xml = withEntityDeclarations(ris.returnUpTo("</" + qName + ">"), qName);

        // we have the full XML object. if using xpath, run it to get the ID
        if (docIdExpression != null) {
          try (InputStream xmlStream = new ByteArrayInputStream(xml.getBytes())) {
            org.w3c.dom.Document xmlDoc = builder.parse(xmlStream);
            id = docIdExpression.evaluate(xmlDoc.getDocumentElement());
          }
        }
      } catch (XPathExpressionException e) {
        log.warn("Error evaluating docID for xml {}. Document will have a UUID.", xml, e);
      } catch (IOException e) {
        log.error("IOException caught", e);
      }

      if (docIdPathStr != null) {
        id = docIdBuilder.toString();
      }

      // always publish a Document we successfully got *some* usable ID String for; a blank one counts as none
      if (Document.isValidId(id)) {
        Document doc = Document.create(docIdCreator.apply(id));
        doc.setField(outputField, xml);
        internalPublishDocument(doc);
      } else if (!skipEmptyId) {
        // if we didn't get the id from evaluating the docIDExpression / using docIdPathStr, use a random UUID
        id = UUID.randomUUID().toString();

        Document doc = Document.create(docIdCreator.apply(id));
        doc.setField(outputField, xml);
        internalPublishDocument(doc);
      } else {
        log.info("No DocID extracted for xml {}. Since skipEmptyId = true, no Document will be published.", xml);
      }
    }

    if (path.equals(docIdPathStr)) {
      inDocIdPath = false;
    }

    currentPath.pop();
  }

  // a record is cut from the raw bytes, so its entity references are unexpanded and its DOCTYPE is left behind; prefix the
  // declarations it uses (and the entities those use) so the record parses on its own
  private String withEntityDeclarations(String xml, String rootName) {
    if (entities.isEmpty() || xml.indexOf('&') < 0) {
      return xml;
    }

    Set<String> used = new LinkedHashSet<>();
    Deque<String> toScan = new ArrayDeque<>();
    toScan.push(xml);
    while (!toScan.isEmpty()) {
      Matcher matcher = ENTITY_REF.matcher(toScan.pop());
      while (matcher.find()) {
        String name = matcher.group(1);
        if (entities.containsKey(name) && used.add(name)) {
          toScan.push(entities.get(name));
        }
      }
    }
    if (used.isEmpty()) {
      return xml;
    }

    StringBuilder declarations = new StringBuilder("<!DOCTYPE ").append(rootName).append(" [");
    for (String name : used) {
      declarations.append("<!ENTITY ").append(name).append(" \"").append(toEntityValue(entities.get(name))).append("\">");
    }
    return declarations.append("]>").append(xml).toString();
  }

  // inverse of how a literal entity value becomes replacement text: character references and '%' are expanded there, and
  // the value is quoted; general entity references are left alone
  private static String toEntityValue(String replacementText) {
    return replacementText.replace("&#", "&#38;#").replace("%", "&#37;").replace("\"", "&#34;");
  }

  @Override
  public void internalEntityDecl(String name, String value) {
    // parameter entities ('%name') are already expanded in the values that use them; the first declaration wins
    if (!name.startsWith("%")) {
      entities.putIfAbsent(name, value);
    }
  }

  @Override
  public void externalEntityDecl(String name, String publicId, String systemId) {
  }

  @Override
  public void elementDecl(String name, String model) {
  }

  @Override
  public void attributeDecl(String eName, String aName, String type, String mode, String value) {
  }

  private void internalPublishDocument(Document doc) throws SAXException {
    try {
      publisher.publish(doc);
    } catch (Exception e) {
      throw new SAXException("Failed to publish document: " + doc.getId(), e);
    }
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (inDocIdPath) {
      docIdBuilder.append(Arrays.copyOfRange(ch, start, start + length));
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

  public void setXmlIdPath(String documentIDPath)  {
    this.docIdPathStr = documentIDPath;
  }

  public void setXpathIdPath(String xpathIdPath) throws XPathExpressionException {
    this.docIdExpression = xpath.compile(xpathIdPath);
  }

  /**
   * Sets how the id of each published Document is created from the id read from its record. XMLFileHandler passes its
   * createDocId, which applies docIdPrefix.
   */
  public void setDocIdCreator(UnaryOperator<String> docIdCreator) {
    this.docIdCreator = docIdCreator;
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
