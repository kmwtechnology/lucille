package com.kmwllc.lucille.core.fileHandler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.Test;

public class XMLFileHandlerTest {

  @Test
  public void testUnsupportedProcessFileOperation() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "utf-8",
        "outputField", "xml"
    )));

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml";
    File file = new File(filePath);

    assertThrows(FileHandlerException.class, () -> xmlHandler.processFile(new FileInputStream(file), filePath));
  }

  @Test
  public void testStaff() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "utf-8",
        "outputField", "xml"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(2, docs.size());

    assertTrue(docs.get(0).has("xml"));
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>daniel</name>\n" +
        "        <role>software engineer</role>\n" +
        "        <salary currency=\"USD\">3000</salary>\n" +
        "        <bio>I am from San Diego</bio>\n" +
        "    </staff>", docs.get(0).getString("xml"));
  }

  @Test
  public void testStaffWithInfoIDExtraction() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/info/id",
        "encoding", "utf-8",
        "outputField", "xml"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staffWithInfo.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals("1001", docs.get(0).getId());
    assertEquals("1002", docs.get(1).getId());
  }

  @Test
  public void testKoreanEncoding() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "ISO-2022-KR",
        "outputField", "xml"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/korean.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();
    assertEquals(1, docs.size());

    assertTrue(docs.get(0).has("xml"));
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>대니엘</name>\n" +
        "        <role>컴퓨터 과학자</role>\n" +
        "        <salary currency=\"USD\">3000</salary>\n" +
        "        <bio>샌디에고</bio>\n" +
        "    </staff>", docs.get(0).getString("xml"));
  }

  @Test
  public void testJapaneseEncoding() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "ISO-2022-JP",
        "outputField", "xml"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/japanese.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(1, docs.size());

    assertTrue(docs.get(0).has("xml"));
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>ニエル</name>\n" +
        "        <role>コンピュ</role>\n" +
        "        <bio>サンディエゴ</bio>\n" +
        "    </staff>", docs.get(0).getString("xml"));
  }

  @Test
  public void testChineseEncoding() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "utf-16",
        "outputField", "xml_field"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/chinese.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(1, docs.size());

    assertTrue(docs.get(0).has("xml_field"));
    assertEquals("<staff>\n" +
        "        <id>1001</id>\n" +
        "        <name>丹尼尔</name>\n" +
        "        <role>电脑科学家</role>\n" +
        "        <salary currency=\"USD\">3000</salary>\n" +
        "        <bio>圣地亚哥</bio>\n" +
        "    </staff>", docs.get(0).getString("xml_field"));
  }

  @Test
  public void testEncodingError() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "wrongEncoding",
        "outputField", "xml_field"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/chinese.xml";
    File file = new File(filePath);
    assertThrows(FileHandlerException.class, () -> xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath));
  }

  @Test
  public void testIdPathAttribute() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xpathIdPath", "@id",
        "encoding", "utf-8",
        "outputField", "xml_field"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staffIDAttribute.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(3, docs.size());

    assertEquals("1001", docs.get(0).getId());
    assertEquals("1002", docs.get(1).getId());

    // Making sure the third document has a UUID for its ID.
    // (this method would throw an exception if it wasn't a UUID.)
    UUID.fromString(docs.get(2).getId());
  }

  @Test
  public void testIdQualifier() throws Exception {
    // Only the XML element with an id of 1001 should be included.
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xpathIdPath", "id[. = '1001']",
        "encoding", "utf-8",
        "outputField", "xml_field",
        "skipEmptyId", true
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(1, docs.size());
    assertEquals("1001", docs.get(0).getId());
  }

  @Test
  public void testIdAttributeQualifier() throws Exception {
    // Only the XML element with an id attribute of 1001 should be included.
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xpathIdPath", "@id[. = '1001']",
        "encoding", "utf-8",
        "outputField", "xml_field",
        "skipEmptyId", true
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    String filePath = "src/test/resources/FileHandlerTest/XMLFileHandlerTest/staffIDAttribute.xml";
    File file = new File(filePath);
    xmlHandler.processFileAndPublish(publisher, new FileInputStream(file), filePath);

    List<Document> docs = messenger.getDocsSentForProcessing();

    assertEquals(1, docs.size());
    assertEquals("1001", docs.get(0).getId());
  }

  @Test
  public void testInvalidConfig() {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        // cannot specify both
        "xmlIdPath", "/Company/staff/id",
        "xpathIdPath", "@id[. = '1001']",
        "encoding", "utf-8",
        "outputField", "xml_field",
        "skipEmptyId", true
    )));

    assertThrows(IllegalArgumentException.class, () -> FileHandler.create("xml", config));
  }
}
