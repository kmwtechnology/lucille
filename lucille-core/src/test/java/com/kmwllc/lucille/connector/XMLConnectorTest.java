package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.connector.xml.XMLConnector;
import com.kmwllc.lucille.core.*;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.kmwllc.lucille.util.StageFactory;
import com.kmwllc.lucille.stage.XPathExtractor;
import com.kmwllc.lucille.util.FileUtils;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class XMLConnectorTest {

  private StageFactory factory = StageFactory.of(XPathExtractor.class);

  @Test
  public void testStaff() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/staff.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

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
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/nestedstaff.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

    // ensure that in a nested scenario, the nested tag does not get included
    assertEquals(2, docs.size());

    assertTrue(docs.get(0).has("xml"));
  }

  @Test
  public void testKoreanEncoding() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/korean.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

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
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/japanese.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

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
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/chinese.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

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
  public void testURL() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/url.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);

    List<Document> docs = manager.getSavedDocumentsSentForProcessing();

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

  @Test(expected = ConnectorException.class)
  public void testEncodingError() throws Exception {
    Config config = ConfigFactory.parseReader(FileUtils.getReader("classpath:XMLConnectorTest/encodingError.conf"));
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run1", "pipeline1");
    Connector connector = new XMLConnector(config);
    connector.execute(publisher);
  }
}
