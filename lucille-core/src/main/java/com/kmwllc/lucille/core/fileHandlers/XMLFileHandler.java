package com.kmwllc.lucille.core.fileHandlers;

import com.kmwllc.lucille.connector.xml.RecordingInputStream;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Iterator;
import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class XMLFileHandler implements FileHandler {

  private static final Logger log = LoggerFactory.getLogger(XMLFileHandler.class);

  private String xmlRootPath;
  private String xmlIdPath;
  private final String docIdPrefix;
  private String encoding;
  private String outputField;

  private SAXParserFactory spf = null;
  private SAXParser saxParser = null;
  private XMLReader xmlReader = null;

  public XMLFileHandler(Config config) {
    this.xmlRootPath = config.getString("xmlRootPath");
    this.xmlIdPath = config.getString("xmlIdPath");
    this.encoding = config.hasPath("encoding") ? config.getString("encoding") : "utf-8";
    this.outputField = config.hasPath("outputField") ? config.getString("outputField") : "xml";
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  @Override
  public Iterator<Document> processFile(Path path) throws Exception {
    // create the inputStream, urlFiles is never used as we are traversing a file system
    RecordingInputStream ris  = new RecordingInputStream(new FileInputStream(path.toFile()));
    // create the XML handler
    DocumentIteratorHandler handler = new DocumentIteratorHandler(xmlRootPath, xmlIdPath, docIdPrefix, outputField, ris);
    // set content handler
    xmlReader.setContentHandler(handler);
    // parse the file
    ris.setEncoding(encoding);
    try (InputStreamReader inputStreamReader = new InputStreamReader(ris, encoding)) {
      xmlReader.parse(new InputSource(inputStreamReader));
    } catch (SAXException | IOException e) {
      throw new ConnectorException("Unable to parse", e);
    }

    return handler;
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws Exception {
    // create the inputStream using the fileContent
    RecordingInputStream ris = new RecordingInputStream(new ByteArrayInputStream(fileContent));
    // create the XML handler
    DocumentIteratorHandler handler = new DocumentIteratorHandler(xmlRootPath, xmlIdPath, docIdPrefix, outputField, ris);
    // set content handler
    xmlReader.setContentHandler(handler);
    // parse the file
    ris.setEncoding(encoding);
    try (InputStreamReader inputStreamReader = new InputStreamReader(ris, encoding)) {
      xmlReader.parse(new InputSource(inputStreamReader));
    } catch (SAXException | IOException e) {
      throw new ConnectorException("Unable to parse", e);
    }

    return handler;
  }

  @Override
  public void beforeProcessingFile(Path path) throws Exception {
    // validate that it is a xml
    if (!FilenameUtils.getExtension(path.toString()).equalsIgnoreCase("xml")) {
      log.info("File {} is not an XML file", path);
      throw new ConnectorException("File is not an XML file");
    }

    // set up Factory, parser and reader
    setUpParserFactoryIfNeeded();
    setUpSAXParserIfNeeded(spf);
    xmlReader = saxParser.getXMLReader();
  }

  private void setUpSAXParserIfNeeded(SAXParserFactory spf) throws ConnectorException {
    if (saxParser == null) {
      try {
        spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
        saxParser = spf.newSAXParser();
      } catch (ParserConfigurationException | SAXException e) {
        throw new ConnectorException("SAX Parser Error", e);
      }
    }
  }

  private void setUpParserFactoryIfNeeded() {
    if (spf == null) {
      spf = SAXParserFactory.newInstance();
      spf.setNamespaceAware(true);
    }
  }
}
