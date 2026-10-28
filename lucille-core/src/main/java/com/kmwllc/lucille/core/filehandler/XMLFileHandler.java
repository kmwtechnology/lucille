package com.kmwllc.lucille.core.filehandler;

import com.kmwllc.lucille.connector.xml.ChunkingXMLHandler;
import com.kmwllc.lucille.connector.xml.RecordingInputStream;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.typesafe.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.xpath.XPathExpressionException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * <p> Given an InputStream to a .xml file, publishes Documents containing the text of separate elements at a "root path"
 * in the Document. This FileHandler does not support the <code>processFile</code> method, <code>processFileAndPublish</code>
 * must be called. Supports attribute extraction and qualifiers via use of the <code>xpathIdPath</code>.
 *
 * <p> Config Parameters:
 * <ul>
 *   <li><code>xmlRootPath</code> (String): The path to the root of the XML Element(s) you want to publish Documents for. For example, in a file
 *   that contains "&lt;Company&gt;" and, inside it, multiple "&lt;Staff&gt;", each of which you want to publish a Document for, your
 *   path should be <code>/Company/Staff</code>.</li>
 *   <li><code>xmlIdPath</code> (String): The <i>absolute</i> path to the XML element you want to use as the Document ID. For example, <code>/Company/Staff/id</code>.</li>
 *   <li><code>xpathIdPath</code> (String): An xpath expression, <i>relative</i> to <code>xmlRootPath</code>, specifying the XML element
 *   or attribute that will be used to create Document IDs. This path can be a qualifier - be sure to enable <code>skipEmptyId</code>, if so!</li>
 *   <li><code>encoding</code> (String, Optional): The encoding the xml files use. Defaults to utf-8.</li>
 *   <li><code>outputField</code> (String, Optional): The field you want to put the extracted XML text into. Defaults to "xml".</li>
 *   <li><code>skipEmptyId</code> (Boolean, Optional): Whether you want to skip a document when your <code>xmlIdPath</code> or <code>xpathIdPath</code>
 *   evaluates to an empty or null String. You should enable if using an <code>xpathIdPath</code> that is a qualifier. Defaults to false.</li>
 *   <li><code>maxRecordBytes</code> (Integer, Optional): The maximum number of bytes the handler may hold in memory while
 *   reading a single record at <code>xmlRootPath</code>. If exceeded, processing of the file stops with an error; documents
 *   from earlier records will already have been published. The limit is approximate, since bytes read ahead by the parser
 *   are counted too, and it also applies to any content before the first record (so a file where <code>xmlRootPath</code>
 *   never matches will hit it). No limit by default.</li>
 * </ul>
 *
 * <b>Note:</b> You cannot specify both an <code>xmlIdPath</code> and an <code>xpathIdPath</code>. <code>xmlIdPath</code> runs
 * roughly twice as fast as <code>xpathIdPath</code>, but is less versatile.
 */
public class XMLFileHandler extends BaseFileHandler {

  public static final Spec SPEC = SpecBuilder.fileHandler()
      .requiredString("xmlRootPath")
      .optionalString("xmlIdPath", "xpathIdPath", "docIdPrefix", "outputField", "encoding")
      .optionalNumber("maxRecordBytes")
      .optionalBoolean("skipEmptyId").build();

  private static final Logger log = LoggerFactory.getLogger(XMLFileHandler.class);

  private final String xmlRootPath;
  private final String xmlIdPath;
  private final String xpathIdPath;
  private final String encoding;
  private final String outputField;
  private final boolean skipEmptyId;
  // 0 means no limit
  private final int maxRecordBytes;

  private SAXParserFactory spf = null;
  private SAXParser saxParser = null;
  private XMLReader xmlReader = null;

  public XMLFileHandler(Config config) {
    super(config);

    this.xmlRootPath = config.getString("xmlRootPath");
    this.xmlIdPath = ConfigUtils.getStringOrDefault(config, "xmlIdPath", null);
    this.xpathIdPath = ConfigUtils.getStringOrDefault(config, "xpathIdPath", null);
    this.encoding = config.hasPath("encoding") ? config.getString("encoding") : "utf-8";
    this.outputField = config.hasPath("outputField") ? config.getString("outputField") : "xml";
    this.skipEmptyId = ConfigUtils.getOrDefault(config, "skipEmptyId", false);

    this.maxRecordBytes = config.hasPath("maxRecordBytes") ? config.getInt("maxRecordBytes") : 0;
    if (config.hasPath("maxRecordBytes") && maxRecordBytes <= 0) {
      throw new IllegalArgumentException("maxRecordBytes must be positive, got " + maxRecordBytes);
    }

    if (config.hasPath("xmlIdPath") == config.hasPath("xpathIdPath")) {
      throw new IllegalArgumentException("Must specify exactly one of xmlIdPath and xpathIdPath.");
    }
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
  }

  @Override
  public void processFileAndPublish(Publisher publisher, InputStream inputStream, String pathStr) throws FileHandlerException {
    // set up Factory, parser, reader and handler
    ChunkingXMLHandler xmlHandler = setUpParserReaderAndHandlerIfNeeded(publisher, pathStr);

    RecordingInputStream ris = new RecordingInputStream(inputStream, maxRecordBytes);

    setEncodingAndParse(xmlHandler, ris, pathStr);
  }

  private void setEncodingAndParse(ChunkingXMLHandler xmlHandler, RecordingInputStream ris, String path) throws FileHandlerException {
    ris.setEncoding(encoding);
    try (InputStreamReader inputStreamReader = new InputStreamReader(ris, encoding)) {
      InputSource xmlSource = new InputSource(inputStreamReader);
      xmlHandler.setRis(ris);
      xmlReader.parse(xmlSource);
    } catch (UnsupportedEncodingException e) {
      throw new FileHandlerException(String.format("%s is not supported as an encoding. (%s)", encoding, path), e);
    } catch (SAXException | IOException e) {
      // the parser may wrap the IOException thrown by the RecordingInputStream
      if (ExceptionUtils.indexOfType(e, RecordingInputStream.BufferLimitExceededException.class) != -1) {
        throw new FileHandlerException(String.format("XML record exceeds maxRecordBytes of %d bytes. (%s)", maxRecordBytes, path), e);
      }
      throw new FileHandlerException(String.format("Unable to parse. (%s)", path), e);
    }
  }

  private void setUpSAXParserIfNeeded(SAXParserFactory spf, String path) throws FileHandlerException {
    if (saxParser == null) {
      try {
        spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
        saxParser = spf.newSAXParser();
      } catch (ParserConfigurationException | SAXException e) {
        throw new FileHandlerException(String.format("SAX Parser Error. (%s)", path), e);
      }
    }
  }

  private void setUpParserFactoryIfNeeded() {
    if (spf == null) {
      spf = SAXParserFactory.newInstance();
      spf.setNamespaceAware(true);
    }
  }

  private ChunkingXMLHandler setUpParserReaderAndHandlerIfNeeded(Publisher publisher, String path) throws FileHandlerException {
    setUpParserFactoryIfNeeded();
    setUpSAXParserIfNeeded(spf, path);
    try {
      xmlReader = saxParser.getXMLReader();
    } catch (SAXException e) {
      throw new FileHandlerException(String.format("unable to get XML Reader. (%s)", path), e);
    }

    ChunkingXMLHandler xmlHandler;

    try {
      xmlHandler = new ChunkingXMLHandler();
    } catch (Exception e) {
      throw new FileHandlerException(String.format("Error setting up ChunkingXMLHandler. (%s)", path), e);
    }

    if (xmlIdPath != null) {
      xmlHandler.setXmlIdPath(xmlIdPath);
    }

    if (xpathIdPath != null) {
      try {
        xmlHandler.setXpathIdPath(xpathIdPath);
      } catch (XPathExpressionException e) {
        throw new FileHandlerException(String.format("XPath related error with your xpathIdPath. (%s)", path), e);
      }
    }

    xmlHandler.setOutputField(outputField);
    xmlHandler.setPublisher(publisher);
    xmlHandler.setDocumentRootPath(xmlRootPath);
    xmlHandler.setDocIDPrefix(docIdPrefix);
    xmlHandler.setSkipEmptyId(skipEmptyId);

    xmlReader.setContentHandler(xmlHandler);
    return xmlHandler;
  }
}
