package com.kmwllc.lucille.connector.storageclient;

import static org.junit.Assert.assertEquals;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.junit.Rule;
import org.junit.Test;

public class StorageClientStateManagerTest {

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "", "",
      "sm-db-test-start.sql", "sm-db-test-end.sql");

  @Test
  public void testStateManagerRootDirectory() throws Exception {
    assertEquals(1, dbHelper.checkNumConnections());

    Config config = ConfigFactory.parseResourcesAnySyntax("StorageClientStateManagerTest/config.conf");
    StorageClientStateManager manager = new StorageClientStateManager(config);

    manager.init();

    manager.shutdown();
  }
}
