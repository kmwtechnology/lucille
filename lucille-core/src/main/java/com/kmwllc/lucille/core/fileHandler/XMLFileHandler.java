package com.kmwllc.lucille.core.fileHandler;

import com.kmwllc.lucille.connector.xml.ChunkingXMLHandler;
import com.kmwllc.lucille.connector.xml.RecordingInputStream;
import com.kmwllc.lucille.core.ConfigUtils;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
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
 *   <li><code>skipEmptyID</code> (Boolean, Optional): Whether you want to skip a document when your <code>xmlIdPath</code> or <code>xpathIdPath</code>
 *   evaluates to an empty or null String. You should enable if using an <code>xpathIdPath</code> that is a qualifier. Defaults to false.</li>
 * </ul>
 *
 * <b>Note:</b> You cannot specify both an <code>xmlIdPath</code> and an <code>xpathIdPath</code>. <code>xmlIdPath</code> runs
 * roughly twice as fast as <code>xpathIdPath</code>, but is less versatile.
 */
public class XMLFileHandler extends BaseFileHandler {

  private static final Logger log = LoggerFactory.getLogger(XMLFileHandler.class);

  private final String xmlRootPath;
  private final String xmlIdPath;
  private final String xpathIdPath;
  private final String encoding;
  private final String outputField;
  private final boolean skipEmptyId;

  private SAXParserFactory spf = null;
  private SAXParser saxParser = null;
  private XMLReader xmlReader = null;

  public XMLFileHandler(Config config) {
    super(config, Spec.fileHandler()
        .withRequiredProperties("xmlRootPath")
        .withOptionalProperties("xmlIdPath", "xpathIdPath", "docIdPrefix", "outputField", "encoding", "skipEmptyId"));

    this.xmlRootPath = config.getString("xmlRootPath");
    this.xmlIdPath = ConfigUtils.getOrDefault(config, "xmlIdPath", null);
    this.xpathIdPath = ConfigUtils.getOrDefault(config, "xpathIdPath", null);
    this.encoding = config.hasPath("encoding") ? config.getString("encoding") : "utf-8";
    this.outputField = config.hasPath("outputField") ? config.getString("outputField") : "xml";
    this.skipEmptyId = ConfigUtils.getOrDefault(config, "skipEmptyId", false);

    if (config.hasPath("xmlIdPath") == config.hasPath("xpathIdPath")) {
      throw new IllegalArgumentException("Must specify exactly one of xmlIdPath and xpathIdPath.");
    }

    this.docIdPrefix = config.hasPath("docIdPrefix") ? config.getString("docIdPrefix") : "";
  }

  @Override
  public Iterator<Document> processFile(InputStream inputStream, String pathStr) throws FileHandlerException {
    throw new FileHandlerException("Unsupported Operation");
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

    ChunkingXMLHandler xmlHandler;

    try {
      xmlHandler = new ChunkingXMLHandler();
    } catch (Exception e) {
      throw new FileHandlerException("Error setting up ChunkingXMLHandler.", e);
    }

    if (xmlIdPath != null) {
      xmlHandler.setXmlIdPath(xmlIdPath);
    }

    if (xpathIdPath != null) {
      try {
        xmlHandler.setXpathIdPath(xpathIdPath);
      } catch (XPathExpressionException e) {
        throw new FileHandlerException("XPath related error with your xpathIdPath.", e);
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
