package com.kmwllc.lucille.connector.xml;

import com.kmwllc.lucille.connector.AbstractConnector;
import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.Spec;
import com.kmwllc.lucille.core.fileHandler.XMLFileHandler;
import com.kmwllc.lucille.util.FileContentFetcher;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.List;


/**
 * Connector implementation that produces documents from a given XML file.
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
@Deprecated
public class XMLConnector extends AbstractConnector {

  private static final Logger log = LoggerFactory.getLogger(XMLConnector.class);

  private List<String> filePaths;
  private List<String> urlFiles;

  private XMLFileHandler xmlFileHandler;


  public XMLConnector(Config config) {
    super(config, Spec.connector()
        .withRequiredProperties("xmlRootPath", "xmlIdPath", "encoding", "outputField")
        .withOptionalProperties("filePaths", "urlFiles"));
    filePaths = config.hasPath("filePaths") ? config.getStringList("filePaths") : null;
    urlFiles = config.hasPath("urlFiles") ? config.getStringList("urlFiles") : null;
    this.xmlFileHandler = new XMLFileHandler(config);
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    if (urlFiles != null) {
      processFilesBasedOnType(publisher, urlFiles, true);
      return;
    }

    processFilesBasedOnType(publisher, filePaths, false);
  }
  
  private void processFilesBasedOnType(Publisher publisher, List<String> files, boolean isUrlFile) throws ConnectorException {
    for (String fileStr : files) {
      if (isUrlFile && fileStr.startsWith("file://")) {
        fileStr = fileStr.replaceFirst("file://", "");
      }

      try {
        InputStream stream = FileContentFetcher.getOneTimeInputStream(fileStr);
        xmlFileHandler.processFileAndPublish(publisher, stream, fileStr);
      } catch (Exception e) {
        throw new ConnectorException("Error processing file: " + fileStr, e);
      }
    }
  }
}
