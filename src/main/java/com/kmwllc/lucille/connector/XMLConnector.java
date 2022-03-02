package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.ConnectorException;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.typesafe.config.Config;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.util.ArrayList;
import java.util.List;

public class XMLConnector extends AbstractConnector {
  private final String path;
  private final List<String> idFields;

  public XMLConnector(Config config) {
    super(config);
    this.path = config.getString("path");
    String idField = config.hasPath("idField") ? config.getString("idField") : null;
    // Either specify the idField, or idFields
    if (idField != null) {
      this.idFields = new ArrayList<String>();
      this.idFields.add(idField);
    } else {
      this.idFields = config.hasPath("idFields") ? config.getStringList("idFields") : new ArrayList<String>();
    }
  }

  @Override
  public void execute(Publisher publisher) throws ConnectorException {
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = null;
    try {
      builder = factory.newDocumentBuilder();
    } catch (Exception e) {
      throw new ConnectorException();
    }

    org.w3c.dom.Document document = builder.parse(ClassLoader.getSystemResourceAsStream(path));

    NodeList nodeList = document.getDocumentElement().getChildNodes();

    for (int i = 0; i < nodeList.getLength(); i++) {
      Node node = nodeList.item(i);

      if (node instanceof Element) {
        Document d = new Document(Integer.toString(i));
        NodeList childNodes = node.getChildNodes();
        for (int j = 0; i < childNodes.getLength(); j++) {
          Node field = childNodes.item(j);
          String value = field.getLastChild().getTextContent().trim();
          d.setField(field.getNodeName(), value);
        }
        publisher.publish(d);
      }


    }

  }

  public String toString() {
    return "XMLConnector: " + path;
  }
}
