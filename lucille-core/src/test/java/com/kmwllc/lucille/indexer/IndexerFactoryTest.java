package com.kmwllc.lucille.indexer;

import com.kmwllc.lucille.core.Indexer;
import com.kmwllc.lucille.core.IndexerContext;
import com.kmwllc.lucille.core.IndexerException;
import com.kmwllc.lucille.core.spec.Spec;
import com.kmwllc.lucille.core.spec.SpecBuilder;
import com.kmwllc.lucille.message.TestMessenger;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.File;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
    File outputFile = new File(config.getString("indexer.csv.path"));
    FileUtils.deleteQuietly(outputFile);
    Indexer indexer = IndexerFactory.fromConfig(config, messenger, true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);
    indexer.closeConnection();

    // creating the indexer does not create its output file
    Assert.assertFalse(outputFile.exists());
  }

  @Test
  public void testFailsWhenNoTypeOrClass() throws Exception {
    assertNoIndexerSelected(ConfigFactory.load("IndexerFactoryTest/config_no_type_or_class.conf"));
  }

  @Test
  public void testFailsWhenSendDisabledAndNothingSelected() throws Exception {
    assertNoIndexerSelected(ConfigFactory.parseString("indexer { sendEnabled: false }"));
  }

  @Test
  public void testFailsWhenBackendBlockIsOnlyAtTopLevel() throws Exception {
    assertNoIndexerSelected(ConfigFactory.parseString(
        "indexer { batchSize: 100 }\ncsv { columns: [\"id\"], path: \"target/x.csv\" }"));
  }

  @Test
  public void testExplicitNopType() throws Exception {
    Config config = ConfigFactory.parseString("indexer { type: \"Nop\" }");
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof NopIndexer);
  }

  @Test
  public void testNestedBlockSelectsIndexer() throws Exception {
    Config config = ConfigFactory.parseString("indexer { csv { columns: [\"id\"], path: \"target/x.csv\" } }");
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);
    indexer.closeConnection();
  }

  @Test
  public void testSeveralNestedBlocksRequireType() {
    Config config = ConfigFactory.parseString("indexer { csv { columns: [\"id\"], path: \"target/x.csv\" }, nop {} }");
    Exception exception = assertThrows(IndexerException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Set indexer.type or indexer.class"));
  }

  @Test
  public void testTypeChoosesAmongNestedBlocks() throws Exception {
    Config config = ConfigFactory.parseString(
        "indexer { type: \"CSV\", csv { columns: [\"id\"], path: \"target/x.csv\" }, nop {} }");
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);
    indexer.closeConnection();
  }

  @Test
  public void testUnregisteredNestedBlockIsAnError() {
    Config config = ConfigFactory.parseString("indexer { unknownBackend { url: \"x\" } }");
    Exception exception = assertThrows(IndexerException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("indexer.unknownBackend"));
  }

  @Test
  public void testUnknownBlockIsAnError() {
    Config config = ConfigFactory.parseString(
        "indexer { type: \"CSV\", csv { columns: [\"id\"], path: \"target/x.csv\" }, filter { blacklist: [\"secret\"] } }");
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Unknown config block indexer.filter"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("CSV"));
  }

  @Test
  public void testMisspelledBlockIsAnError() {
    Config config = ConfigFactory.parseString("indexer { type: \"CSV\", csvv { columns: [\"id\"], path: \"target/x.csv\" } }");
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Unknown config block indexer.csvv"));
  }

  @Test
  public void testBlockForAnotherRegisteredIndexerIsAllowed() throws Exception {
    Config config = ConfigFactory.parseString("indexer { type: \"Nop\", csv { columns: [\"id\"], path: \"target/x.csv\" } }");
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof NopIndexer);
    indexer.closeConnection();
  }

  @Test
  public void testTopLevelBlockOfUnannotatedIndexerIsAnError() {
    UnannotatedIndexer.closed = 0;
    Config config = ConfigFactory.parseString("indexer { class: \"" + UnannotatedIndexer.class.getName() + "\" }, my { size: 1 }");
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Move the block inside the indexer block"));
    assertEquals(1, UnannotatedIndexer.closed);
  }

  @Test
  public void testNestedBlockOfUnannotatedIndexerIsAllowed() throws Exception {
    Config config = ConfigFactory.parseString("my { size: 1 }, indexer { class: \"" + UnannotatedIndexer.class.getName() + "\", my: ${my} }").resolve();
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof UnannotatedIndexer);
    indexer.closeConnection();
  }

  // Has no @IndexerType, so its block name is only known from getIndexerConfigKey().
  public static class UnannotatedIndexer extends NopIndexer {

    public static final Spec SPEC = SpecBuilder.indexer().optionalNumber("size").build();

    static int closed = 0;

    public UnannotatedIndexer(Config config, IndexerContext context) {
      super(config, context);
    }

    @Override
    protected String getIndexerConfigKey() {
      return "my";
    }

    @Override
    public void closeConnection() {
      closed++;
    }
  }

  @Test
  public void testTopLevelBlockIsAnError() {
    Config config = ConfigFactory.parseString(
        "indexer { type: \"CSV\" }, csv { columns: [\"id\"], path: \"target/x.csv\" }");
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("Move the block inside the indexer block"));
  }

  @Test
  public void testTopLevelBlockIsAllowedWhenNestedBlockExists() throws Exception {
    Config config = ConfigFactory.parseString(
        "csv { columns: [\"id\"] }, indexer { csv: ${csv} { path: \"target/x.csv\" } }").resolve();
    Indexer indexer = IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing");
    Assert.assertTrue(indexer instanceof CSVIndexer);
    indexer.closeConnection();
  }

  @Test
  public void testWronglyCasedBlockIsAnError() {
    Config config = ConfigFactory.parseString("indexer { CSV { columns: [\"id\"], path: \"target/x.csv\" } }");
    Exception exception = assertThrows(IllegalArgumentException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("must be named indexer.csv"));
  }

  @Test
  public void testFailsWhenNoIndexerBlock() throws Exception {
    assertNoIndexerSelected(ConfigFactory.load("IndexerFactoryTest/config_no_indexer_block.conf"));
  }

  private static void assertNoIndexerSelected(Config config) {
    Exception exception = assertThrows(IndexerException.class,
        () -> IndexerFactory.fromConfig(config, new TestMessenger(), true, "testing"));
    Assert.assertTrue(exception.getMessage(), exception.getMessage().contains("No indexer is selected"));
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
