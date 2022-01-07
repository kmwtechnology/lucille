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

public class S3ConnectorTest {

  // Connector should be able to use any VFS compatible protocol including relative local files
  @Test
  public void testExecuteWithLocalFiles() throws Exception {
    Config config = ConfigFactory.load("S3ConnectorTest/localfs.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run", "pipeline1");
    Connector connector = new S3Connector(config);
    connector.execute(publisher);
    Assert.assertEquals(4, manager.getSavedDocumentsSentForProcessing().size());
    String[] fileNames = {"localfs.conf", "localfs_filtered.conf", "s3config.conf"};
    for (Document doc : manager.getSavedDocumentsSentForProcessing()) {
      String filePath = doc.getString(FileTraverser.FILE_PATH);
      String content = new String(Base64.getDecoder().decode(doc.getString(DefaultDocumentProducer.CONTENT)));
      Assert.assertTrue(Arrays.stream(fileNames).anyMatch(filePath::endsWith));
      if (filePath.endsWith("s3config.conf")) {
        Assert.assertTrue(content.contains("name: \"s3-connector-1\""));
      }
    }
  }

  // Connector should be able to use any VFS compatible protocol including relative local files
  @Test
  public void testExecuteWithLocalFilesFiltered() throws Exception {
    Config config = ConfigFactory.load("S3ConnectorTest/localfs_filtered.conf");
    PersistingLocalMessageManager manager = new PersistingLocalMessageManager();
    Publisher publisher = new PublisherImpl(config, manager, "run", "pipeline1");
    Connector connector = new S3Connector(config);
    connector.execute(publisher);
    Assert.assertEquals(1, manager.getSavedDocumentsSentForProcessing().size());
    Document doc = manager.getSavedDocumentsSentForProcessing().get(0);
    Assert.assertTrue(doc.getString(FileTraverser.FILE_PATH).endsWith("localfs_filtered.conf"));
    byte[] decodedBytes = Base64.getDecoder().decode(doc.getString(DefaultDocumentProducer.CONTENT));
    String decodedString = new String(decodedBytes);
    Assert.assertTrue(decodedString.contains("name: \"vfs-connector-1\""));
  }

}