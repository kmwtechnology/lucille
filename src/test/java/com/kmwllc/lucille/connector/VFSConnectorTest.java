package com.kmwllc.lucille.connector;

import com.kmwllc.lucille.core.Connector;
import com.kmwllc.lucille.core.Document;
import com.kmwllc.lucille.core.Publisher;
import com.kmwllc.lucille.core.PublisherImpl;
import com.kmwllc.lucille.filetraverser.FileTraverser;
import com.kmwllc.lucille.filetraverser.data.producer.DefaultDocumentProducer;
import com.kmwllc.lucille.message.PersistingLocalMessageManager;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Base64;

public class VFSConnectorTest {

  // Connector should be able to use any VFS compatible protocol including relative local files
  @Test
  public void testExecuteWithLocalFiles() throws Exception {
    Config config = ConfigFactory.load("VFSConnectorTest/localfs.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run", "pipeline1");
    Connector connector = new VFSConnector(config);
    connector.execute(publisher);
    String[] fileNames = {"a.json", "b.json", "c.json", "d.json", "e.json", "e.json.gz", "e.yaml", "f.jsonl"};
    int docCount = 0;
    for (Document doc : manager.getSavedDocumentsSentForProcessing()) {
      String docId = doc.getId();
      String filePath = doc.getString(FileTraverser.FILE_PATH);
      // skip if it's an automatically generated Finder file because the directory was opened
      if (filePath.endsWith(".DS_Store")) continue;
      String content = new String(Base64.getDecoder().decode(doc.getString(DefaultDocumentProducer.CONTENT)));
      Assert.assertTrue(docId.startsWith("file_"));
      Assert.assertTrue(Arrays.stream(fileNames).anyMatch(filePath::endsWith));
      if (filePath.endsWith("c.json")) {
        Assert.assertTrue(content.contains("\"artist\":\"Lou Levit\""));
      }
      docCount++;
    }
    Assert.assertEquals(8, docCount);
  }

  // Connector should be able to use any VFS compatible protocol including relative local files
  @Test
  public void testExecuteWithLocalFilesFiltered() throws Exception {
    Config config = ConfigFactory.load("VFSConnectorTest/localfs_filtered.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run", "pipeline1");
    Connector connector = new VFSConnector(config);
    connector.execute(publisher);
    Assert.assertEquals(3, manager.getSavedDocumentsSentForProcessing().size());
    String[] fileNames = {"a.json", "b.json", "c.json"};
    for (Document doc : manager.getSavedDocumentsSentForProcessing()) {
      String docId = doc.getId();
      String filePath = doc.getString(FileTraverser.FILE_PATH);
      String content = new String(Base64.getDecoder().decode(doc.getString(DefaultDocumentProducer.CONTENT)));
      Assert.assertTrue(Arrays.stream(fileNames).anyMatch(filePath::endsWith));
      if (filePath.endsWith("a.json")) Assert.assertTrue(content.contains("\"filename\":\"400_106547e2f83b.jpg\""));
      if (filePath.endsWith("b.json")) Assert.assertTrue(content.contains("\"imageHash\":\"1aaeac2de7c48e4e7773b1f92138291f\""));
      if (filePath.endsWith("c.json")) Assert.assertTrue(content.contains("\"productImg\":\"mug-400_6812876c6c27.jpg\""));
    }
  }

}