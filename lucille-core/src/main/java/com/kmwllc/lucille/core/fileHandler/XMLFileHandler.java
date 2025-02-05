package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.connector.xml.ChunkingXMLHandler;
import com.kmwllc.lucille.connector.xml.RecordingInputStream;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.nio.file.Path;
import java.util.Iterator;
import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

public class XMLFileHandler extends BaseFileHandler {

  private static final Logger log = LoggerFactory.getLogger(XMLFileHandler.class);

  private String xmlRootPath;
  private String xmlIdPath;
  private String encoding;
  private String outputField;

  private SAXParserFactory spf = null;
  private SAXParser saxParser = null;
  private XMLReader xmlReader = null;

  public XMLFileHandler(Config config) {
    super(config);
    this.xmlRootPath = config.getString("xmlRootPath");
    this.xmlIdPath = config.getString("xmlIdPath");
    this.encoding = config.hasPath("encoding") ? config.getString("encoding") : "utf-8";
    this.outputField = config.hasPath("outputField") ? config.getString("outputField") : "xml";
    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  @Override
  public Iterator<Document> processFile(Path path) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }

  @Override
  public Iterator<Document> processFile(byte[] fileContent, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }

  @Override
  public void processFileAndPublish(Publisher publisher, Path path) throws FileHandlerException {
    // set up Factory, parser, reader and handler
    ChunkingXMLHandler xmlHandler = setUpParserReaderAndHandlerIfNeeded(publisher);

    RecordingInputStream ris;
    try {
      ris = new RecordingInputStream(new FileInputStream(path.toFile()));
    } catch (FileNotFoundException | SecurityException e) {
      throw new FileHandlerException("Error getting file: " + path, e);
    }

    setEncodingAndParse(xmlHandler, ris);
  }

  @Override
  public void processFileAndPublish(Publisher publisher, byte[] fileContent, String pathStr) throws FileHandlerException {
    // set up Factory, parser, reader and handler
    ChunkingXMLHandler xmlHandler = setUpParserReaderAndHandlerIfNeeded(publisher);

    RecordingInputStream ris = new RecordingInputStream(new ByteArrayInputStream(fileContent));

    setEncodingAndParse(xmlHandler, ris);
  }

  @Override
  public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException {
    // set up Factory, parser, reader and handler
    ChunkingXMLHandler xmlHandler = setUpParserReaderAndHandlerIfNeeded(publisher);

    RecordingInputStream ris = new RecordingInputStream(inputStream);

    setEncodingAndParse(xmlHandler, ris);
  }

  private void setEncodingAndParse(ChunkingXMLHandler xmlHandler, RecordingInputStream ris) throws FileHandlerException {
    ris.setEncoding(encoding);
    try (InputStreamReader inputStreamReader = new InputStreamReader(ris, encoding)) {
      InputSource xmlSource = new InputSource(inputStreamReader);
      xmlHandler.setRis(ris);
      xmlReader.parse(xmlSource);
    } catch (UnsupportedEncodingException e) {
      throw new FileHandlerException(encoding + " is not supported as an encoding", e);
    } catch (SAXException | IOException e) {
      throw new FileHandlerException("Unable to parse", e);
    }
  }

  private void setUpSAXParserIfNeeded(SAXParserFactory spf) throws FileHandlerException {
    if (saxParser == null) {
      try {
        spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
        saxParser = spf.newSAXParser();
      } catch (ParserConfigurationException | SAXException e) {
        throw new FileHandlerException("SAX Parser Error", e);
      }
    }
  }

  private void setUpParserFactoryIfNeeded() {
    if (spf == null) {
      spf = SAXParserFactory.newInstance();
      spf.setNamespaceAware(true);
    }
  }

  private ChunkingXMLHandler setUpParserReaderAndHandlerIfNeeded(Publisher publisher) throws FileHandlerException {
    setUpParserFactoryIfNeeded();
    setUpSAXParserIfNeeded(spf);
    try {
      xmlReader = saxParser.getXMLReader();
    } catch (SAXException e) {
      throw new FileHandlerException("unable to get XML Reader", e);
    }

    ChunkingXMLHandler xmlHandler = new ChunkingXMLHandler();
    xmlHandler.setOutputField(outputField);
    xmlHandler.setPublisher(publisher);
    xmlHandler.setDocumentRootPath(xmlRootPath);
    xmlHandler.setDocumentIDPath(xmlIdPath);
    xmlHandler.setDocIDPrefix(docIdPrefix);
    xmlReader.setContentHandler(xmlHandler);
    return xmlHandler;
  }
}
