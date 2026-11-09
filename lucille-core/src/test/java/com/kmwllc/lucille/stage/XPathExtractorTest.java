package com.kmwllc.lucille.stage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import static org.junit.Assert.assertFalse;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.core.Stage;
import com.kmwllc.lucille.core.StageException;
import com.kmwllc.lucille.core.filehandler.FileHandler;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
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
  public void withXMLFileHandlerTest() throws Exception {
    // pass XML document through XMLFileHandler first
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "utf-8",
        "xmlField", "xml"
    )));
    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");
    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml";
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(new File(filePath)), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    Stage stage = factory.get("XPathExtractorTest/joint.conf");
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
  public void testSpec() throws StageException {
    Stage stage = factory.get("XPathExtractorTest/config.conf");
    assertEquals(Set.of("source", "fieldMapping", "allowDoctype"), stage.getNonDefaultLegalProperties());
  }

  private Document extract(boolean allowDoctype, String xml) throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put("fieldMapping", Map.of("\"/r/v\"", List.of("out")));
    config.put("allowDoctype", allowDoctype);
    Document doc = Document.create("doc");
    doc.setField("xml", xml);
    factory.get(config).processDocument(doc);
    return doc;
  }

  @Test
  public void testDoctypeAndExternalEntities() throws Exception {
    Path secret = Files.createTempFile("xpath-xxe", ".txt");
    secret.toFile().deleteOnExit();
    Files.writeString(secret, "secret-contents");
    String external = "<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY x SYSTEM \"" + secret.toUri() + "\">]><r><v>&x;</v></r>";
    String internal = "<?xml version=\"1.0\"?><!DOCTYPE r [<!ENTITY c \"internal\">]><r><v>&c;</v></r>";

    // a DOCTYPE is rejected by default
    assertFalse(extract(false, external).has("out"));
    assertFalse(extract(false, internal).has("out"));

    // allowDoctype accepts internal entities but never loads external ones
    assertEquals(List.of(""), extract(true, external).getStringList("out"));
    assertEquals(List.of("internal"), extract(true, internal).getStringList("out"));
  }
}
