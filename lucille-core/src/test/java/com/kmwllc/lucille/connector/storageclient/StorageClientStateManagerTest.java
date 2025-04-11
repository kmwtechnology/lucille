package com.kmwllc.lucille.connector.storageclient;

import com.kmwllc.lucille.connector.jdbc.DBTestHelper;
import org.junit.Rule;

public class StorageClientStateManagerTest {

  @Rule
  public final DBTestHelper dbHelper = new DBTestHelper("org.h2.Driver", "jdbc:h2:mem:test", "", "", "db-test-start.sql", "db-test-end.sql");
}
