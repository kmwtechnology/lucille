package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
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
import java.util.regex.Pattern;

public class XMLConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(XMLConnector.class);

  private String fileFolder;
  private String xmlRootPath;
  private String xmlIDPath;
  private String docIDPrefix;
  private String fileRegex;
  private Pattern fileRegexPattern;
  private String urlFile;

  public XMLConnector(Config config) {
    super(config);
    fileFolder = config.hasPath("fileFolder") ? config.getString("fileFolder") : null;
    fileRegex = config.hasPath("fileRegex") ? config.getString("fileRegex") : null;
    xmlRootPath = config.hasPath("XmlRootPath") ? config.getString("XmlRootPath") : null;
    xmlIDPath = config.hasPath("XmlIDPath") ? config.getString("XmlIDPath") : null;
    docIDPrefix = config.hasPath("docIDPrefix") ? config.getString("DocIDPrefix") : "doc_";
    urlFile = config.hasPath("urlFile") ? config.getString("urlFile") : null;
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
      log.warn("SAX Parser Error {}", e);
    }

    try {
      XMLReader xmlReader = saxParser.getXMLReader();
      ChunkingXMLHandler xmlHandler = new ChunkingXMLHandler();
      xmlHandler.setConnector(this);
      xmlHandler.setPublisher(publisher);
      xmlHandler.setDocumentRootPath(xmlRootPath);
      xmlHandler.setDocumentIDPath(xmlIDPath);
      xmlHandler.setDocIDPrefix(docIDPrefix);
      xmlReader.setContentHandler(xmlHandler);

      if (urlFile != null) {
        // We are going to process the file of urls instead of the file directory specification.

        String[] lines = FileUtils.toString(new File(urlFile)).split("\n");
        for (String line : lines) {
          line = line.trim();
          System.out.println("URL TO CRAWL: " + line);
          InputStream in = new URL(line).openStream();
          BufferedInputStream bis = new BufferedInputStream(in);
          RecordingInputStream ris = new RecordingInputStream(bis);
          InputSource xmlSource = new InputSource(ris);
          xmlHandler.setRis(ris);
          xmlReader.parse(xmlSource);
          in.close();
        }


      } else {

        File xmlDir = new File(fileFolder);
        File[] filesToParse = xmlDir.listFiles(new FilenameFilter() {
          @Override
          public boolean accept(File dir, String name) {
            return name.endsWith(".xml");
          }
        });

        for (File file : filesToParse) {
          log.info("Parsing file: {}", file);
          FileInputStream fis = new FileInputStream(file);
          RecordingInputStream ris = new RecordingInputStream(fis);
          InputSource xmlSource = new InputSource(ris);
          xmlHandler.setRis(ris);
          xmlReader.parse(xmlSource);
          // xmlReader.parse(convertToFileURL(filename));
        }
      }
    } catch (IOException | SAXException e) {
      log.warn("SAX Parser Error {}", e);
    }
  }

  public void initialize() {
    if (fileRegex != null) {
      fileRegexPattern = Pattern.compile(fileRegex);
    }
  }
}