package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertThrows;

public class IndexerFactoryTest {

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
  public void testFromValidTypeConfigCSV() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_valid_type_csv.conf");
    File outputFile = new File(config.getString("csv.path"));
    FileUtils.deleteQuietly(outputFile);
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);
    indexer.closeConnection();

    // creating the indexer does not create its output file
    Assert.assertFalse(outputFile.exists());
  }

  @Test
  public void testDefaultsToNopIndexerWhenNoTypeOrClass() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_no_type_or_class.conf");

    // With neither indexer.class nor indexer.type set, the factory defaults to a no-op indexer
    // rather than throwing, so a pipeline can run without a configured destination.
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof NopIndexer);
  }

  @Test
  public void testDefaultsToNopIndexerWhenNoIndexerBlock() throws Exception {
    TestMessenger messenger = new TestMessenger();
    Config config = ConfigFactory.load("IndexerFactoryTest/config_no_indexer_block.conf");

    // should still default to NopIndexer rather than throwing ConfigException.Missing when indexer block is missing
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof NopIndexer);
  }

  @Test
  public void testSelectIndexerClassRejectsDuplicateTypes() {
    List<Class<? extends Indexer>> matches = List.of(CSVIndexer.class, CSVIndexer.class);
    Exception exception = assertThrows(IndexerException.class,
        () -> IndexerFactory.selectIndexerClass("Dup", matches, List.of("Dup")));
    Assert.assertTrue(exception.getMessage().contains("Ambiguous indexer.type configuration of:"));
    // The message lists the names of the conflicting class(es) so the user can find them.
    Assert.assertTrue(exception.getMessage().contains(CSVIndexer.class.getName()));
  }

  @Test
  public void testSelectIndexerClassRejectsUnknownType() {
    Exception exception = assertThrows(IndexerException.class,
        () -> IndexerFactory.selectIndexerClass("Missing", List.of(), List.of("CSV", "Nop")));
    Assert.assertTrue(exception.getMessage().contains("Unknown indexer.type configuration of:"));
  }

  @Test
  public void testSelectIndexerClassReturnsSingleMatch() throws Exception {
    Class<? extends Indexer> selected =
        IndexerFactory.selectIndexerClass("CSV", List.of(CSVIndexer.class), List.of("CSV"));
    Assert.assertEquals(CSVIndexer.class, selected);
  }
}
