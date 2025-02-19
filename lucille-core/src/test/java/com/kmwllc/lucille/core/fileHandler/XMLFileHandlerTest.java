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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml");
    byte[] fileContent = Files.readAllBytes(path);

    assertThrows(FileHandlerException.class, () -> xmlHandler.processFile(path));
    assertThrows(FileHandlerException.class, () -> xmlHandler.processFile(fileContent, path.toString()));
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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/staff.xml");
    xmlHandler.processFileAndPublish(publisher, path);

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
  public void testNestedStaff() throws Exception {
    Config config = ConfigFactory.parseMap(Map.of("xml", Map.of(
        "xmlRootPath", "/Company/staff",
        "xmlIdPath", "/Company/staff/id",
        "encoding", "utf-8",
        "outputField", "xml"
    )));

    TestMessenger messenger = new TestMessenger();
    Publisher publisher = new PublisherImpl(config, messenger, "run1", "pipeline1");

    FileHandler xmlHandler = FileHandler.create("xml", config);
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/nestedstaff.xml");
    xmlHandler.processFileAndPublish(publisher, path);

    List<Document> docs = messenger.getDocsSentForProcessing();

    // ensure that in a nested scenario, the nested tag does not get included
    assertEquals(2, docs.size());

    assertTrue(docs.get(0).has("xml"));
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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/korean.xml");
    xmlHandler.processFileAndPublish(publisher, path);

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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/japanese.xml");
    xmlHandler.processFileAndPublish(publisher, path);

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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/chinese.xml");
    xmlHandler.processFileAndPublish(publisher, path);

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
    Path path = Paths.get("src/test/resources/FileHandlerTest/XMLFileHandlerTest/chinese.xml");
    assertThrows(FileHandlerException.class, () -> xmlHandler.processFileAndPublish(publisher, path));
  }
}
