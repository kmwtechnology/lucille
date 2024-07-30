package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class IndexerFactoryTest {

  @Test
  public void testFromDefaultTypeConfig() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_default_type.conf");
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof SolrIndexer);
  }

  @Test
  public void testFromValidTypeConfig() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_valid_type.conf");
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof SolrIndexer);
  }

  @Test
  public void testFromInvalidTypeConfig() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_invalid_type.conf");
    Exception exception = assertThrows(IndexerException.class, () -> {
      Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    });
    Assert.assertTrue(exception.getMessage().contains("Unknown indexer.type configuration of:"));
  }

  @Test
  public void testFromValidTypeConfigElastic() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_valid_type_elastic.conf");
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof ElasticsearchIndexer);
  }

  @Test
  public void testFromValidTypeConfigCSV() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_valid_type_csv.conf");
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);

    // removing the foo file created from test
    boolean fooIsDeleted = FileUtils.deleteQuietly(new File(config.getString("csv.path")));
    Assert.assertTrue(fooIsDeleted);
  }
}
