package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

import javax.xml.XMLConstants;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import java.io.*;
import java.net.URL;
import java.util.List;


/**
 * Connector implementation that produces documents from a given XML file.
 * <p>
 * Config Parameters:
 * <ul>
 * <li>filePaths (List&lt;String&gt;): The list of file paths to parse through.</li>
 * <li>xmlRootPath (String): The path to the XML chunk to separate as a document.</li>
 * <li>xmlIdPath (String): The path to the id for each document.</li>
 * <li>urlFiles (List&lt;String&gt;): The list of URL file paths to parse. If specified along with filePaths, urlFiles takes precedence.</li>
 * <li>encoding (String): The encoding of the XML document to parse: defaults to utf-8.</li>
 * <li>outputField (String): The field to place the XML into: defaults to "xml".</li>
 * </ul>
 */
public class XMLConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(XMLConnector.class);

  private List<String> filePaths;
  private String xmlRootPath;
  private String xmlIdPath;
  private List<String> urlFiles;
  private String encoding;
  private String outputField;


  public XMLConnector(Config config) {
    super(config);
    filePaths = config.hasPath("filePaths") ? config.getStringList("filePaths") : null;
    xmlRootPath = config.hasPath("xmlRootPath") ? config.getString("xmlRootPath") : null;
    xmlIdPath = config.hasPath("xmlIdPath") ? config.getString("xmlIdPath") : null;
    urlFiles = config.hasPath("urlFiles") ? config.getStringList("urlFiles") : null;
    encoding = config.hasPath("encoding") ? config.getString("encoding") : "utf-8";
    outputField = config.hasPath("outputField") ? config.getString("outputField") : "xml";
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    SAXParserFactory spf = SAXParserFactory.newInstance();
    spf.setNamespaceAware(true);
    SAXParser saxParser = null;
    try {
      spf.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, false);
      saxParser = spf.newSAXParser();
    } catch (ParserConfigurationException | SAXException e) {
      throw new ConnectorException("SAX Parser Error", e);
    }

    XMLReader xmlReader = null;
    try {
      xmlReader = saxParser.getXMLReader();
    } catch (SAXException e) {
      throw new ConnectorException("Error during SAX Processing", e);
    }

    ChunkingXMLHandler xmlHandler = new ChunkingXMLHandler();
    xmlHandler.setOutputField(outputField);
    xmlHandler.setConnector(this);
    xmlHandler.setPublisher(publisher);
    xmlHandler.setDocumentRootPath(xmlRootPath);
    xmlHandler.setDocumentIDPath(xmlIdPath);
    xmlHandler.setDocIDPrefix(getDocIdPrefix());
    xmlReader.setContentHandler(xmlHandler);

    if (urlFiles != null) {
      // We are going to process the file of urls instead of the file directory specification.
      for (String file : urlFiles) {
        log.info("URL TO CRAWL: {}", file);
        if (file.startsWith("file://")) {
          file = file.replaceFirst("file://", "");
        }
        File f = new File(file);
        try (InputStream in = new URL("file://" + f.getAbsolutePath()).openStream();
            RecordingInputStream ris = new RecordingInputStream(in);) {
          setUpAndParse(ris, xmlHandler, xmlReader);
        } catch (Exception e) {
          throw new ConnectorException("Error during XML parsing", e);
        }
      }
      return;
    }

    // looking for filePaths because urlFiles is not specified
    for (String file : filePaths) {
      if (!FilenameUtils.getExtension(file).equalsIgnoreCase("xml")) {
        log.info("File {} is not an XML file", file);
        continue;
      }
      log.info("Parsing file: {}", file);
      try (FileInputStream fis = new FileInputStream(file);
          RecordingInputStream ris = new RecordingInputStream(fis)) {
        setUpAndParse(ris, xmlHandler, xmlReader);
      } catch (Exception e) {
        throw new ConnectorException("Error during XML parsing", e);
      }
    }

  }

  public void setUpAndParse(RecordingInputStream ris, ChunkingXMLHandler xmlHandler, XMLReader xmlReader)
      throws ConnectorException {
    ris.setEncoding(encoding);
    try (InputStreamReader inputStreamReader = new InputStreamReader(ris, encoding)) {
      InputSource xmlSource = new InputSource(inputStreamReader);
      xmlHandler.setRis(ris);
      xmlReader.parse(xmlSource);
    } catch (UnsupportedEncodingException e) {
      throw new ConnectorException(encoding + " is not supported as an encoding", e);
    } catch (SAXException | IOException e) {
      throw new ConnectorException("Unable to parse", e);
    }
  }
}
