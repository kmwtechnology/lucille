package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.connector.xml.ChunkingXMLHandler;
import com.kmwllc.lucille.connector.xml.RecordingInputStream;
import com.kmwllc.lucille.core.Document;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Stack;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

public class DocumentIteratorHandler extends DefaultHandler implements Iterator<Document> {
  private static final Logger log = LoggerFactory.getLogger(ChunkingXMLHandler.class);

  Stack<String> currentPath = new Stack<String>();
  private String documentRootPath;
  private String documentIDPath;
  private String docIDPrefix = "";
  private boolean inDocID = false;
  private StringBuilder docIDBuilder = new StringBuilder();
  private RecordingInputStream ris;
  private String outputField;
  private BlockingQueue<Document> docQueue = new LinkedBlockingQueue<>();
  private boolean parsingComplete = false;
  private Document currentDoc = null;

  public DocumentIteratorHandler(String documentRootPath, String documentIDPath,
      String docIDPrefix, String outputField,
      RecordingInputStream ris) {
    this.documentRootPath = documentRootPath;
    this.documentIDPath = documentIDPath;
    this.docIDPrefix = docIDPrefix;
    this.outputField = outputField;
    this.ris = ris;
  }

  @Override
  public void startElement(String uri, String localName, String qName, Attributes atts) {
    // push on the stack.
    currentPath.push(qName);
    log.info("qName: {}", qName);
    String path = "/" + StringUtils.join(currentPath.toArray(), "/");
    log.info("Current path: {}", path);
    log.info("documentRootPath: {}", documentRootPath);
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
    log.info("path in endElement: {}", path);
    if (documentRootPath.equals(path)) {
      String xml = "Malformed";
      try {
        xml = ris.returnUpTo("</" + qName + ">");
      } catch (IOException e) {
        log.error("IOException caught", e);
      }
      log.info("xml: {}", xml);
      // XPath not supported where id is in attribute (e.g. <field name="id">
      // Quick workaround is to use a UUID instead and assign ID elsewhere in workflow
      String id = docIDBuilder.toString();
      if (id == null || id.equals("")) {
        id = UUID.randomUUID().toString();
      }

      Document doc = Document.create(docIDPrefix + id);
      doc.setField(outputField, xml);
      try {
        docQueue.put(doc);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
    if (documentIDPath.equals(path)) {
      // this is the end of the doc id tag.
      inDocID = false;
    }

    currentPath.pop();
  }

  @Override
  public void characters(char[] ch, int start, int length) throws SAXException {
    if (inDocID) {
      docIDBuilder.append(Arrays.copyOfRange(ch, start, start + length));
    }
  }

  @Override
  public void endDocument() throws SAXException {
    // mark parsing as complete
    parsingComplete = true;
  }

  @Override
  public boolean hasNext() {
    // if current document is null, try to fetch next
    if (currentDoc == null) {
      try {
        // if queue is empty and parsing is complete, return false
        if (parsingComplete && docQueue.isEmpty()) {
          try {
            ris.close();
          } catch (IOException e) {
            log.error("Error while closing input stream", e);
          }
          return false;
        }

        // block and wait for next document if needed
        currentDoc = docQueue.take();
        return true;
      } catch (InterruptedException e) {
        log.error("Error while waiting for next document to be parsed", e);
        try {
          ris.close();
        } catch (IOException ex) {
          log.error("Error while closing input stream", ex);
        }
        return false;
      }
    }
    return true;
  }

  @Override
  public Document next() {
    if (!hasNext()) {
      throw new NoSuchElementException("No more documents to parse");
    }

    // return current document and reset
    Document doc = currentDoc;
    currentDoc = null;
    return doc;
  }
}
