package com.kmwllc.lucille.stage;

import com.kmwllc.lucille.connector.xml.XMLConnector;
import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class XPathExtractorTest {

  private StageFactory factory = StageFactory.of(XPathExtractor.class);

  @Test
  public void testXPathExtractor() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("xml",
      "<bookstore>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Harry Potter</title>\n" +
      "  <price>29.99</price>\n" +
      "</book>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Learning XML</title>\n" +
      "  <price>39.95</price>\n" +
      "</book>\n" +
      "\n" +
      "</bookstore>");

    stage.processDocument(doc1);

    List<String> results = doc1.getStringList("output1");

    assertEquals("Harry Potter", results.get(0));
    assertEquals("Learning XML", results.get(1));

    List<String> results2 = doc1.getStringList("output2");

    assertEquals("Harry Potter", results2.get(0));
    assertEquals("Learning XML", results2.get(1));
  }

  @Test
  public void testSpecifiedXmlField() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/specifyxml.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("random", "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "\n" +
      "<bookstore>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Harry Potter</title>\n" +
      "  <price>29.99</price>\n" +
      "</book>\n" +
      "\n" +
      "<book>\n" +
      "  <title lang=\"en\">Learning XML</title>\n" +
      "  <price>39.95</price>\n" +
      "</book>\n" +
      "\n" +
      "</bookstore>");

    stage.processDocument(doc1);

    List<String> results = doc1.getStringList("output1");

    assertEquals("Harry Potter", results.get(0));
    assertEquals("Learning XML", results.get(1));
  }

  @Test
  public void testKorean() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/config.conf");

    Document doc1 = Document.create("doc1");
    doc1.setField("xml",
      "<bookstore>\n" +
        "\n" +
        "<book>\n" +
        "  <title lang=\"en\">해리 포터</title>\n" +
        "  <price>29.99</price>\n" +
        "</book>\n" +
        "\n" +
        "<book>\n" +
        "  <title lang=\"en\">" +
        "XML 학습</title>\n" +
        "  <price>39.95</price>\n" +
        "</book>\n" +
        "\n" +
        "</bookstore>");

    stage.processDocument(doc1);

    List<String> results = doc1.getStringList("output1");

    assertEquals("해리 포터", results.get(0));
    assertEquals("XML 학습", results.get(1));
  }

  @Test
  public void withXMLConnectorTest() throws Exception {
    // pass XML document through XMLConnector first
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/staff.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

    Stage stage = factory.get("XMLConnectorTest/joint.conf");
    stage.processDocument(docs.get(0));
    stage.processDocument(docs.get(1));

    assertEquals("daniel", docs.get(0).getString("name"));
    assertEquals("software engineer", docs.get(0).getString("role"));
    assertEquals("I am from San Diego", docs.get(0).getString("bio"));
    assertEquals("brian", docs.get(1).getString("name"));
    assertEquals("admin", docs.get(1).getString("role"));
    assertEquals("I enjoy reading", docs.get(1).getString("bio"));
  }

  @Test
  public void testGetLegalProperties() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/config.conf");
    assertEquals(Set.of("xmlField", "name", "conditions", "class"), stage.getLegalProperties());
  }
}
